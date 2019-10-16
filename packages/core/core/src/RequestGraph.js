// @flow strict-local

import type {FilePath, Glob, JSONObject} from '@parcel/types';
import type {Event} from '@parcel/watcher';
import type {Config, ParcelOptions, Target} from './types';
import type {AbortSignal} from 'abortcontroller-polyfill/dist/cjs-ponyfill';
import type {
  Asset as AssetValue,
  AssetRequestDesc,
  AssetRequestResult,
  AssetRequestNode,
  ConfigRequest,
  ConfigRequestNode,
  Dependency,
  DepPathRequestNode,
  DepVersionRequestNode,
  EntryRequestNode,
  NodeId,
  RequestGraphNode,
  RequestNode,
  SubRequestNode,
  TargetRequestNode,
  TransformationOpts,
  ValidationOpts
} from './types';

import invariant from 'assert';
import nullthrows from 'nullthrows';
import path from 'path';

import {PromiseQueue, md5FromObject, isGlob, isGlobMatch} from '@parcel/utils';
import WorkerFarm from '@parcel/workers';

import {addDevDependency} from './InternalConfig';
import ConfigLoader from './ConfigLoader';
import Graph, {type GraphOpts} from './Graph';
import type ParcelConfig from './ParcelConfig';
import ResolverRunner from './ResolverRunner';
import {EntryResolver} from './EntryResolver';
import type {EntryResult} from './EntryResolver'; // ? Is this right
import TargetResolver from './TargetResolver';
import type {TargetResolveResult} from './TargetResolver';
import {assertSignalNotAborted} from './utils';

type RequestGraphOpts = {|
  ...GraphOpts<RequestGraphNode>,
  config: ParcelConfig,
  options: ParcelOptions,
  onEntryRequestComplete: (string, Array<FilePath>) => mixed,
  onTargetRequestComplete: (FilePath, Array<Target>) => mixed,
  onAssetRequestComplete: (AssetRequestNode, Array<AssetValue>) => mixed,
  onDepPathRequestComplete: (DepPathRequestNode, AssetRequest | null) => mixed,
  workerFarm: WorkerFarm
|};

type SerializedRequestGraph = {|
  ...GraphOpts<RequestGraphNode>,
  invalidNodeIds: Set<NodeId>,
  globNodeIds: Set<NodeId>,
  depVersionRequestNodeIds: Set<NodeId>
|};

const nodeFromDepPathRequest = (dep: Dependency) => ({
  id: dep.id,
  type: 'dep_path_request',
  value: dep
});

const nodeFromConfigRequest = (configRequest: ConfigRequest) => ({
  id: md5FromObject({
    filePath: configRequest.filePath,
    plugin: configRequest.plugin,
    env: configRequest.env
  }),
  type: 'config_request',
  value: configRequest
});

const nodeFromDepVersionRequest = depVersionRequest => ({
  id: md5FromObject(depVersionRequest),
  type: 'dep_version_request',
  value: depVersionRequest
});

const nodeFromFilePath = (filePath: string) => ({
  id: filePath,
  type: 'file',
  value: {filePath}
});

const nodeFromGlob = (glob: Glob) => ({
  id: glob,
  type: 'glob',
  value: glob
});

const nodeFromRequest = (request: Request) => ({
  id: request.id,
  type: request.type,
  value: request
});

export default class RequestGraph extends Graph<RequestGraphNode> {
  // $FlowFixMe
  inProgress: Map<NodeId, Promise<any>> = new Map();
  invalidNodeIds: Set<NodeId> = new Set();
  runTransform: TransformationOpts => Promise<AssetRequestResult>;
  runValidate: ValidationOpts => Promise<void>;
  loadConfigHandle: () => Promise<Config>;
  entryResolver: EntryResolver;
  targetResolver: TargetResolver;
  resolverRunner: ResolverRunner;
  configLoader: ConfigLoader;
  onEntryRequestComplete: (string, Array<FilePath>) => mixed;
  onTargetRequestComplete: (FilePath, Array<Target>) => mixed;
  onAssetRequestComplete: (AssetRequestNode, Array<AssetValue>) => mixed;
  onDepPathRequestComplete: (DepPathRequestNode, AssetRequest | null) => mixed;
  queue: PromiseQueue<mixed>;
  validationQueue: PromiseQueue<mixed>;
  farm: WorkerFarm;
  config: ParcelConfig;
  options: ParcelOptions;
  globNodeIds: Set<NodeId> = new Set();
  // Unpredictable nodes are requests that cannot be predicted whether they should rerun based on
  // filesystem changes alone. They should rerun on each startup of Parcel.
  unpredicatableNodeIds: Set<NodeId> = new Set();
  depVersionRequestNodeIds: Set<NodeId> = new Set();
  signal: ?AbortSignal;

  // $FlowFixMe
  static deserialize(opts: SerializedRequestGraph) {
    let deserialized = new RequestGraph(opts);
    deserialized.invalidNodeIds = opts.invalidNodeIds;
    deserialized.globNodeIds = opts.globNodeIds;
    deserialized.depVersionRequestNodeIds = opts.depVersionRequestNodeIds;
    deserialized.unpredicatableNodeIds = opts.unpredicatableNodeIds;
    // $FlowFixMe
    return deserialized;
  }

  // $FlowFixMe
  serialize(): SerializedRequestGraph {
    return {
      ...super.serialize(),
      invalidNodeIds: this.invalidNodeIds,
      globNodeIds: this.globNodeIds,
      unpredicatableNodeIds: this.unpredicatableNodeIds,
      depVersionRequestNodeIds: this.depVersionRequestNodeIds
    };
  }

  initOptions({
    onAssetRequestComplete,
    onDepPathRequestComplete,
    onEntryRequestComplete,
    onTargetRequestComplete,
    config,
    options,
    workerFarm
  }: RequestGraphOpts) {
    this.options = options;
    this.queue = new PromiseQueue();
    this.validationQueue = new PromiseQueue();
    this.onAssetRequestComplete = onAssetRequestComplete;
    this.onDepPathRequestComplete = onDepPathRequestComplete;
    this.onEntryRequestComplete = onEntryRequestComplete;
    this.onTargetRequestComplete = onTargetRequestComplete;
    this.config = config;

    this.entryResolver = new EntryResolver(this.options);
    this.targetResolver = new TargetResolver(this.options);

    this.resolverRunner = new ResolverRunner({
      config,
      options
    });

    this.farm = workerFarm;
    this.runTransform = this.farm.createHandle('runTransform');
    this.runValidate = this.farm.createHandle('runValidate');
    // $FlowFixMe
    this.loadConfigHandle = this.farm.createReverseHandle(
      this.loadConfig.bind(this)
    );
    this.configLoader = new ConfigLoader(options);
  }

  async completeValidations() {
    await this.validationQueue.run();
  }

  async completeRequests(signal?: AbortSignal) {
    this.signal = signal;

    for (let id of this.invalidNodeIds) {
      let node = nullthrows(this.getNode(id));
      this.processNode(node);
    }

    await this.queue.run();
  }

  addNode(node: RequestGraphNode) {
    if (!this.hasNode(node.id)) {
      this.processNode(node);

      if (node.type === 'glob') {
        this.globNodeIds.add(node.id);
      } else if (node.type === 'dep_version_request') {
        this.depVersionRequestNodeIds.add(node.id);
      }
    }

    return super.addNode(node);
  }

  removeNode(node: RequestGraphNode) {
    this.invalidNodeIds.delete(node.id);
    if (node.type === 'glob') {
      this.globNodeIds.delete(node.id);
    } else if (node.type === 'dep_version_request') {
      this.depVersionRequestNodeIds.delete(node.id);
    } else if (node.type === 'config_request') {
      this.unpredicatableNodeIds.delete(node.id);
    }
    return super.removeNode(node);
  }

  addRequestNode(request: Request) {
    let requestNode = nodeFromRequest(request);
    if (!this.hasNode(requestNode.id)) {
      this.addNode(requestNode);
    }
  }

  addEntryRequest(entry: string) {
    let request = new EntryRequest({
      request: entry,
      entryResolver: this.entryResolver
    });
    this.addRequestNode(request);
  }

  addTargetRequest(entry: FilePath) {
    let request = new TargetRequest({
      request: entry,
      targetResolver: this.targetResolver
    });
    this.addRequestNode(request);
  }

  addDepPathRequest(dep: Dependency) {
    let requestNode = nodeFromDepPathRequest(dep);
    if (!this.hasNode(requestNode.id)) {
      this.addNode(requestNode);
    }
  }

  addAssetRequest(id: NodeId, request: AssetRequest) {
    let request = new AssetRequest({
      request
    });
    let requestNode = {id, type: 'asset_request', value: request};
    if (!this.hasNode(requestNode.id)) {
      this.addNode(requestNode);
    }

    this.connectFile(requestNode, request.filePath);
  }

  async processNode2(requestNode) {
    let signal = this.signal; // ? is this safe?
    let promise = this.queue.add(async () => {
      let result = await requestNode.value.run();
      assertSignalNotAborted(signal);
      if (!this.hasNode(requestNode)) {
        return;
      }
      requestNode.value.addResultToGraph(result, this);
    });

    if (promise) {
      try {
        this.inProgress.set(requestNode.id, promise);
        await promise;
        // ? Should these be updated before it comes off the queue?
        this.invalidNodeIds.delete(requestNode.id);
      } catch (e) {
        // Do nothing
        // Main tasks will be caught by the queue
        // Sub tasks will end up rejecting the main task promise
      } finally {
        this.inProgress.delete(requestNode.id);
      }
    }
  }

  async processNode(requestNode: RequestGraphNode) {
    let promise;
    switch (requestNode.type) {
      case 'entry_request':
        promise = this.queue.add(() => this.resolveEntry(requestNode));
        break;
      case 'target_request':
        promise = this.queue.add(() => this.resolveTargetRequest(requestNode));
        break;
      case 'asset_request':
        promise = this.queue.add(() => this.transform(requestNode));

        if (
          !requestNode.value.filePath.includes('node_modules') &&
          this.config.getValidatorNames(requestNode.value.filePath).length > 0
        ) {
          this.validationQueue.add(() => this.validate(requestNode));
        }

        break;
      case 'dep_path_request':
        promise = this.queue.add(() =>
          this.resolvePath(requestNode.value).then(result => {
            if (result) {
              this.onDepPathRequestComplete(requestNode, result);
            }
            return result;
          })
        );
        break;
      case 'config_request':
        promise = this.runConfigRequest(requestNode);
        break;
      case 'dep_version_request':
        promise = this.runDepVersionRequest(requestNode);
        break;
      default:
        this.invalidNodeIds.delete(requestNode.id);
    }

    if (promise) {
      try {
        this.inProgress.set(requestNode.id, promise);
        await promise;
        // ? Should these be updated before it comes off the queue?
        this.invalidNodeIds.delete(requestNode.id);
      } catch (e) {
        // Do nothing
        // Main tasks will be caught by the queue
        // Sub tasks will end up rejecting the main task promise
      } finally {
        this.inProgress.delete(requestNode.id);
      }
    }
  }

  validate(requestNode: AssetRequestNode) {
    return this.runValidate({
      request: requestNode.value,
      loadConfig: this.loadConfigHandle,
      parentNodeId: requestNode.id,
      options: this.options
    });
  }

  async resolvePath(dep: Dependency) {
    let assetRequest = await this.resolverRunner.resolve(dep);
    if (assetRequest) {
      this.connectFile(nodeFromDepPathRequest(dep), assetRequest.filePath);
    }
    return assetRequest;
  }

  async loadConfig(configRequest: ConfigRequest, parentNodeId: NodeId) {
    if (!this.hasNode(parentNodeId)) {
      return this.configLoader.load(configRequest);
    }

    let configRequestNode = nodeFromConfigRequest(configRequest);
    if (!this.hasNode(configRequestNode.id)) {
      this.addNode(configRequestNode);
    }
    this.addEdge(parentNodeId, configRequestNode.id);

    let config = nullthrows(await this.getSubTaskResult(configRequestNode));
    invariant(config.devDeps != null);

    // ConfigRequest node might have been deleted while waiting for config to resolve
    if (!this.hasNode(configRequestNode.id)) {
      return config;
    }

    let depVersionRequestNodes = [];
    for (let [moduleSpecifier, version] of config.devDeps) {
      let depVersionRequest = {
        moduleSpecifier,
        resolveFrom: config.resolvedPath, // TODO: resolveFrom should be nearest package boundary
        result: version
      };
      let depVersionRequestNode = nodeFromDepVersionRequest(depVersionRequest);
      if (!this.hasNode(depVersionRequestNode.id) || version) {
        this.addNode(depVersionRequestNode);
      }
      this.addEdge(configRequestNode.id, depVersionRequestNode.id);
      depVersionRequestNodes.push(
        nullthrows(this.getNode(depVersionRequestNode.id))
      );

      if (version == null) {
        let result = await this.getSubTaskResult(depVersionRequestNode);
        addDevDependency(config, depVersionRequest.moduleSpecifier, result);
      }
    }
    this.replaceNodesConnectedTo(
      configRequestNode,
      depVersionRequestNodes,
      node => node.type === 'dep_version_request'
    );

    return config;
  }

  async runConfigRequest(configRequestNode: ConfigRequestNode) {
    let configRequest = configRequestNode.value;
    let config = await this.configLoader.load(configRequest);

    // Config request could have been deleted while loading
    if (!this.hasNode(configRequestNode.id)) {
      return config;
    }

    configRequest.result = config;

    let invalidationNodes = [];
    if (config.resolvedPath != null) {
      invalidationNodes.push(nodeFromFilePath(config.resolvedPath));
    }

    for (let filePath of config.includedFiles) {
      invalidationNodes.push(nodeFromFilePath(filePath));
    }

    if (config.watchGlob != null) {
      invalidationNodes.push(nodeFromGlob(config.watchGlob));
    }

    this.replaceNodesConnectedTo(
      configRequestNode,
      invalidationNodes,
      node => node.type === 'file' || node.type === 'glob'
    );

    if (config.shouldInvalidateOnStartup) {
      this.unpredicatableNodeIds.add(configRequestNode.id);
    } else {
      this.unpredicatableNodeIds.delete(configRequestNode.id);
    }

    return config;
  }

  async runDepVersionRequest(requestNode: DepVersionRequestNode) {
    let {value: request} = requestNode;
    let {moduleSpecifier, resolveFrom, result} = request;

    let version = result;

    if (version == null) {
      let {pkg} = await this.options.packageManager.resolve(
        `${moduleSpecifier}/package.json`,
        `${resolveFrom}/index`
      );

      // TODO: Figure out how to handle when local plugin packages change, since version won't be enough
      version = nullthrows(pkg).version;
      request.result = version;
    }

    return version;
  }

  //$FlowFixMe
  async getSubTaskResult(node: SubRequestNode): any {
    let result;
    if (this.inProgress.has(node.id)) {
      result = await this.inProgress.get(node.id);
    } else {
      result = this.getResultFromGraph(node);
    }

    return result;
  }

  getResultFromGraph(subRequestNode: SubRequestNode) {
    let node = nullthrows(this.getNode(subRequestNode.id));
    invariant(
      node.type === 'config_request' || node.type === 'dep_version_request'
    );
    return node.value.result;
  }

  connectFile(requestNode: RequestNode, filePath: FilePath) {
    if (!this.hasNode(requestNode.id)) {
      return;
    }

    let fileNode = nodeFromFilePath(filePath);
    if (!this.hasNode(fileNode.id)) {
      this.addNode(fileNode);
    }

    if (!this.hasEdge(requestNode.id, fileNode.id)) {
      this.addEdge(requestNode.id, fileNode.id);
    }
  }

  connectGlob(requestNode: RequestNode, glob: Glob) {
    if (!this.hasNode(requestNode.id)) {
      return;
    }

    let globNode = nodeFromGlob(glob);
    if (!this.hasNode(globNode.id)) {
      this.addNode(globNode);
    }

    if (!this.hasEdge(requestNode.id, globNode.id)) {
      this.addEdge(requestNode.id, globNode.id);
    }
  }

  invalidateNode(node: RequestNode) {
    switch (node.type) {
      case 'asset_request':
      case 'dep_path_request':
      case 'entry_request':
      case 'target_request':
        this.invalidNodeIds.add(node.id);
        break;
      case 'config_request':
      case 'dep_version_request': {
        this.invalidNodeIds.add(node.id);
        let mainRequestNode = nullthrows(this.getMainRequestNode(node));
        this.invalidNodeIds.add(mainRequestNode.id);
        break;
      }
      default:
        throw new Error(
          `Cannot invalidate node with unrecognized type ${node.type}`
        );
    }
  }

  invalidateUnpredictableNodes() {
    for (let nodeId of this.unpredicatableNodeIds) {
      let node = nullthrows(this.getNode(nodeId));
      invariant(node.type !== 'file' && node.type !== 'glob');
      this.invalidateNode(node);
    }
  }

  getMainRequestNode(node: SubRequestNode) {
    let [parentNode] = this.getNodesConnectedTo(node);
    if (parentNode.type === 'config_request') {
      [parentNode] = this.getNodesConnectedTo(parentNode);
    }
    invariant(parentNode.type !== 'file' && parentNode.type !== 'glob');
    return parentNode;
  }

  // TODO: add edge types to make invalidation more flexible and less precarious
  respondToFSEvents(events: Array<Event>): boolean {
    let isInvalid = false;
    for (let {path, type} of events) {
      if (path === this.options.lockFile) {
        for (let id of this.depVersionRequestNodeIds) {
          let depVersionRequestNode = this.getNode(id);
          invariant(
            depVersionRequestNode &&
              depVersionRequestNode.type === 'dep_version_request'
          );

          this.invalidateNode(depVersionRequestNode);
          isInvalid = true;
        }
      }

      let node = this.getNode(path);

      let connectedNodes =
        node && node.type === 'file' ? this.getNodesConnectedTo(node) : [];

      // TODO: invalidate dep path requests that have failed and this creation may fulfill the request
      if (node && (type === 'create' || type === 'update')) {
        // sometimes mac reports update events as create events
        if (node.type === 'file') {
          for (let connectedNode of connectedNodes) {
            if (
              connectedNode.type === 'asset_request' ||
              connectedNode.type === 'config_request' ||
              connectedNode.type === 'entry_request' ||
              connectedNode.type === 'target_request'
            ) {
              this.invalidateNode(connectedNode);
              isInvalid = true;
            }
          }
        }
      } else if (type === 'create') {
        for (let id of this.globNodeIds) {
          let globNode = this.getNode(id);
          invariant(globNode && globNode.type === 'glob');

          if (isGlobMatch(path, globNode.value)) {
            let connectedNodes = this.getNodesConnectedTo(globNode);
            for (let connectedNode of connectedNodes) {
              invariant(
                connectedNode.type !== 'file' && connectedNode.type !== 'glob'
              );
              this.invalidateNode(connectedNode);
              isInvalid = true;
            }
          }
        }
      } else if (node && type === 'delete') {
        for (let connectedNode of connectedNodes) {
          if (
            connectedNode.type === 'dep_path_request' ||
            connectedNode.type === 'config_request'
          ) {
            this.invalidateNode(connectedNode);
            isInvalid = true;
          }
        }
      }
    }

    return isInvalid;
  }
}

class Request<TRequestDesc: JSONObject | string, TResult> {
  id: string;
  type: string;
  request: TRequestDesc;
  runFn: TRequestDesc => Promise<TResult>;
  storeResult: boolean;
  result: ?TResult;
  promise: ?Promise<TResult>;

  constructor({
    type,
    request,
    runFn,
    storeResult
  }: {|
    type: string,
    request: TRequestDesc,
    runFn: TRequestDesc => Promise<TResult>,
    storeResult?: boolean
  |}) {
    this.id = typeof request === 'string' ? request : md5FromObject(request);
    this.type = type;
    this.request = request;
    this.runFn = runFn;
    this.storeResult = storeResult || true;
  }

  async run(): Promise<TResult> {
    this.result = null;
    this.promise = this.runFn(this.request);
    let result = await this.promise;
    if (this.storeResult) {
      this.result = result;
    }
    this.promise = null;

    return result;
  }

  // vars need to be defined for flow
  addResultToGraph(
    requestNode: RequestNode, // eslint-disable-line no-unused-vars
    result: TResult, // eslint-disable-line no-unused-vars
    graph: RequestGraph // eslint-disable-line no-unused-vars
  ) {
    throw new Error('Request Subclass did not override `addResultToGraph`');
  }
}

class EntryRequest extends Request<FilePath, EntryResult> {
  constructor({
    request,
    entryResolver
  }: {|
    request: FilePath,
    entryResolver: EntryResolver
  |}) {
    super({
      type: 'entry_request',
      request,
      runFn: () => entryResolver.resolveEntry(request),
      storeResult: false
    });
  }

  addResultToGraph(
    requestNode: EntryRequestNode,
    result: EntryResult,
    graph: RequestGraph
  ) {
    // Connect files like package.json that affect the entry
    // resolution so we invalidate when they change.
    for (let file of result.files) {
      graph.connectFile(requestNode, file.filePath);
    }

    // If the entry specifier is a glob, add a glob node so
    // we invalidate when a new file matches.
    if (isGlob(requestNode.value)) {
      graph.connectGlob(requestNode, requestNode.value);
    }

    //this.onEntryRequestComplete(entryRequestNode.value, result.entries);
  }
}

class TargetRequest extends Request<FilePath, TargetResolveResult> {
  constructor({
    request,
    targetResolver
  }: {|
    request: FilePath,
    targetResolver: TargetResolver
  |}) {
    super({
      type: 'target_request',
      request,
      runFn: () => targetResolver.resolve(path.dirname(request)),
      storeResult: false
    });
  }

  addResultToGraph(
    requestNode: TargetRequestNode,
    result: TargetResolveResult,
    graph: RequestGraph
  ) {
    // Connect files like package.json that affect the target
    // resolution so we invalidate when they change.
    for (let file of result.files) {
      this.connectFile(targetRequestNode, file.filePath);
    }

    //this.onTargetRequestComplete(targetRequestNode.value, result.targets);
  }
}

class AssetRequest extends Request<AssetRequestDesc, AssetRequestResult> {
  constructor({
    request,
    runTransform,
    loadConfig,
    parentNodeId,
    options
  }: {|
    request: AssetRequestDesc,
    // TODO: get shared flow type
    runTransform: ({|
      request: AssetRequestDesc,
      loadConfig: (ConfigRequest, NodeId) => Promise<Config>,
      parentNodeId: NodeId,
      options: ParcelOptions,
      workerApi: WorkerApi // ? Does this need to be here?
    |}) => Promise<AssetRequestResult>,
    loadConfig: any, //TODO
    parentNodeId: NodeId,
    options: ParcelOptions
  |}) {
    super({
      type: 'asset_request',
      request,
      runFn: async () => {
        let start = Date.now();
        let {assets, configRequests} = await runTransform({
          request,
          loadConfig,
          parentNodeId: this.id, // ? Will this be the right value
          options
        });

        let time = Date.now() - start;
        for (let asset of assets) {
          asset.stats.time = time;
        }
        return {assets, configRequests};
      }
    });
  }

  addResultToGraph(requestNode, result, graph) {
    let {assets, configRequests} = result;
    let configRequestNodes = configRequests.map(configRequest => {
      let node = nodeFromConfigRequest(configRequest);
      return graph.getNode(node.id) || node;
    });
    graph.replaceNodesConnectedTo(
      requestNode,
      configRequestNodes,
      node => node.type === 'config_request'
    );

    //this.onAssetRequestComplete(requestNode, assets);
    return assets;

    // TODO: add includedFiles even if it failed so we can try a rebuild if those files change
  }
}

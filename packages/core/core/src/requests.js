// @flow strict-local
import type {FilePath, JSONObject} from '@parcel/types';
import type RequestGraph from './RequestGraph';
import type {EntryResolver, EntryResult} from './EntryResolver';

import {md5FromObject} from '@parcel/utils';

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

  addResultToGraph(result: TResult, graph: RequestGraph) {
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

  addResultToGraph(result: EntryResult, graph: RequestGraph) {
    // Connect files like package.json that affect the entry
    // resolution so we invalidate when they change.
    for (let file of result.files) {
      graph.connectFile(entryRequestNode, file.filePath);
    }

    // If the entry specifier is a glob, add a glob node so
    // we invalidate when a new file matches.
    if (isGlob(entryRequestNode.value)) {
      graph.connectGlob(entryRequestNode, entryRequestNode.value);
    }
  }
}

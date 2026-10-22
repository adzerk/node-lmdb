'use strict';

const assert = require('assert').strict;
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Worker } = require('worker_threads');
const lmdb = require('..');

const options = directory => ({ path: directory, maxDbs: 2, mapSize: 16 * 1024 * 1024 });
const workerCode = `
const { parentPort, workerData } = require('worker_threads');
const lmdb = require(workerData.module);
const env = new lmdb.Env();
env.open(workerData.options);
const db = env.openDbi({ name: 'shared' });
let held;
parentPort.on('message', op => {
  if (op === 'read') {
    const txn = env.beginTxn({ readOnly: true });
    const value = txn.getString(db, 'key');
    txn.abort();
    parentPort.postMessage(value);
  } else if (op === 'close') {
    db.close();
    env.close();
    parentPort.postMessage('closed');
  } else if (op === 'hold') {
    held = env.beginTxn({ readOnly: true });
    parentPort.postMessage(held.getString(db, 'key'));
  } else if (op === 'exit') process.exit(0);
});
parentPort.postMessage('ready');`;

describe('shared environment refcounts across worker isolates', function() {
  this.timeout(15000);
  let directory, env, db, workers;

  function response(worker) {
    return new Promise((resolve, reject) => {
      const message = value => {
        worker.removeListener('error', error);
        resolve(value);
      };
      const error = failure => {
        worker.removeListener('message', message);
        reject(failure);
      };
      worker.once('message', message);
      worker.once('error', error);
    });
  }

  async function start(count) {
    for (let i = 0; i < count; i++) {
      const worker = new Worker(workerCode, {
        eval: true,
        workerData: { module: path.resolve(__dirname, '..'), options: options(directory) }
      });
      workers.push(worker);
      assert.equal(await response(worker), 'ready');
    }
  }

  function send(worker, op) {
    const result = response(worker);
    worker.postMessage(op);
    return result;
  }

  function read() {
    const txn = env.beginTxn({ readOnly: true });
    try { return txn.getString(db, 'key'); }
    finally { txn.abort(); }
  }

  beforeEach(function() {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'lmdb-refcount-'));
    workers = [];
    env = new lmdb.Env();
    env.open(options(directory));
    db = env.openDbi({ name: 'shared', create: true });
    const txn = env.beginTxn();
    txn.putString(db, 'key', 'alive');
    txn.commit();
  });

  afterEach(async function() {
    await Promise.all(workers.map(worker => worker.terminate()));
    if (db) {
      db.close();
      env.close();
    }
    fs.rmSync(directory, { recursive: true, force: true });
  });

  it('retains the environment when one of two wrappers closes, including the original opener', async function() {
    await start(2);
    assert.equal(await send(workers[0], 'close'), 'closed');
    assert.equal(await send(workers[1], 'read'), 'alive');
    db.close();
    env.close();
    db = null;
    assert.equal(await send(workers[1], 'read'), 'alive');
  });

  it('releases a thread-exit reference/read transaction without closing survivor wrappers', async function() {
    await start(4);
    assert.equal(await send(workers[0], 'hold'), 'alive');
    const exited = new Promise(resolve => workers[0].once('exit', resolve));
    workers[0].postMessage('exit');
    assert.equal(await exited, 0);
    for (const worker of workers.slice(1)) assert.equal(await send(worker, 'read'), 'alive');
    assert.equal(await send(workers[1], 'close'), 'closed');
    for (const worker of workers.slice(2)) assert.equal(await send(worker, 'read'), 'alive');
    assert.equal(read(), 'alive');
  });

  it('reopens the same path after the final references exit without explicit close', async function() {
    await start(2);
    db.close();
    env.close();
    db = null;
    await Promise.all(workers.map(worker => worker.terminate()));
    env = new lmdb.Env();
    env.open(options(directory));
    db = env.openDbi({ name: 'shared' });
    assert.equal(read(), 'alive');
  });
});

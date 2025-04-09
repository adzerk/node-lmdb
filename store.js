const { pack, Unpackr } = require('msgpackr')
const { mkdirSync }         = require('fs')
const { Cursor, Env }       = require('node-gyp-build')(__dirname)
const { AsyncLocalStorage } = require('async_hooks')

const BINARY_DATA_KEY = '\x10binary-data\x02'

const unpackr = new Unpackr({
  useRecords: false,
  int64AsType: 'auto', // convert msgpack int64s below Number.MAX_SAFE_INTEGER to Number
})

function asBinary(buffer) {
  return {
    [BINARY_DATA_KEY]: buffer,
  }
}

function startsWithBuffer(buffer, prefix) {
  if (buffer.length < prefix.length) {
    return false
  }

  for (let i = 0; i < prefix.length; i++) {
    if (buffer[i] !== prefix[i]) {
      return false
    }
  }

  return true
}


class Iterator {
  constructor(env, dbi, options = {}) {
    this.txn = env.beginTxn({ readOnly: true })
    this.cursor = new Cursor(this.txn, dbi)
    this.options = options
    this.prefixBuffer = options.prefix ? Buffer.from(options.prefix) : null
  }

  close() {
    this.cursor.close()
    this.txn.abort() // a readonly txn
  }

  [Symbol.iterator]() {
    const self = this
    const { keys = true, values = true } = this.options
    let key = this.prefixBuffer ? this.cursor.goToRange(this.prefixBuffer) : this.cursor.goToFirst()

    return {
      next() {
        if (key !== null && (!self.prefixBuffer || startsWithBuffer(key, self.prefixBuffer))) {
          let value
          if (keys && values) {
            value = {
              key: key.toString('utf8'),
              value: unpackr.unpack(self.cursor.getCurrentBinary())
            }
          } else if (keys) {
            value = key.toString('utf8')
          } else if (values) {
            value = unpackr.unpack(self.cursor.getCurrentBinary())
          } else {
            value = null
          }
          key = self.cursor.goToNext()
          return { value, done: false }
        } else {
          self.close()
          return { done: true }
        }
      }
    }
  }
}

/**
 * This class enforces a very specific usage of lmdb. All keys are
 * utf8 encoded buffers and all values are encoded with
 * msgpack. Only one named dbi is supported. Transaction 
 * handling is also intentionally simplified to
 * just one current transaction, no nesting. This is the pattern that
 * adset-consumer uses in its current form.
 */
class Store {
  // TODO: add in support for a persistent readonly transaction for the lifetime
  // of the Store instance - Engines will need this
  // TODO: maybe refactor to split env/dbi params
  // TODO: support additional dbis

  /**
   * @param {object}  options
   * @param {boolean} options.create
   * @param {number}  options.mapSize
   * @param {string}  options.name
   * @param {boolean} options.noReadAhead
   * @param {string}  options.path
   * @param {AsyncLocalStorage} context
   */
  constructor({
    create = false,
    mapSize,
    name,
    noReadAhead = false,
    path,
    readOnly = false,
  }, context) {
    this.env = new Env()
    this.env.open({ path, mapSize, noReadAhead, readOnly })
    this.dbi = this.env.openDbi({ name, create, keyIsBuffer: true })

    if (context) {
      this.context = context
    } else {
      this.context = new AsyncLocalStorage()
    }
  }

  /**
   *
   * @param {string} key
   * @param {object} value
   */
  put(key, value) {
    this.transact((txn) => {
      const keyBuffer = Buffer.from(key, 'utf8')
      let valueBuffer
      if (value && value[BINARY_DATA_KEY])
        valueBuffer = value[BINARY_DATA_KEY]
      else
        valueBuffer = pack(value)
      txn.putBinary(this.dbi, keyBuffer, valueBuffer)
    })
  }

  /**
   *
   * @param {object} key
   * @returns
   */
  get(key) {
    return this.transact((txn) => {
      const keyBuffer = Buffer.from(key, 'utf8')
      const value = txn.getBinary(this.dbi, keyBuffer)
      return value == null ? null : unpackr.unpack(value)
    }, true)
  }

  /**
   *
   * @param {object} key
   * @returns
   */
  del(key) {
    return this.transact((txn) => {
      const keyBuffer = Buffer.from(key, 'utf8')
      if (txn.getBinary(this.dbi, keyBuffer) != null) {
        txn.del(this.dbi, keyBuffer)
        return true
      } else {
        return false
      }
    })
  }

  /**
   * Wrapper function for a transaction.
   * @param {*} f Function to execute within a txn
   * @param {boolean} [readOnly=false] Set to true if txn is readOnly
   */
  transact(f, readOnly = false) {
    let ownTxn = false
    let store = this.context.getStore()
    if (!store) {
      store = {}
      this.context.enterWith(store)
    }
    let txn = store.txn
    if (!txn) {
      txn = this.env.beginTxn({ readOnly })
      store.txn = txn
      ownTxn = true
    }

    try {
      const result = f(txn)
      if (ownTxn) {
        if (readOnly)
          txn.abort()
        else
          txn.commit()
      }
      return result
    } catch (error) {
      if (ownTxn) {
        console.error('transaction aborted:', error.message)
        txn.abort()
      }
      throw error
    } finally {
      if (ownTxn)
        store.txn = null
    }
  }

  async transactAsync(f, readOnly = false) {
    let ownTxn = false
    let store = this.context.getStore()
    if (!store) {
      store = {}
      this.context.enterWith(store)
    }
    let txn = store.txn
    if (!txn) {
      txn = this.env.beginTxn({ readOnly })
      store.txn = txn
      ownTxn = true
    }

    try {
      const result = await f(txn)
      if (ownTxn) {
        if (readOnly)
          txn.abort()
        else
          txn.commit()
      }
      return result
    } catch (error) {
      if (ownTxn) {
        console.error('transaction aborted:', error.message)
        txn.abort()
      }
      throw error
    } finally {
      if (ownTxn)
        store.txn = null
    }
  }

  iterate() {
    return new Iterator(this.env, this.dbi)
  }

  getRange(options = {}) {
    return new Iterator(this.env, this.dbi, options)
  }

  getKeys(options = {}) {
    return new Iterator(this.env, this.dbi, { ...options, values: false })
  }

  getMany(keys, callback) {
    // TODO: optimise this: use zero-copy/unsafe buffers
    let results = new Array(keys.length)
    this.transact((txn) => {
      for (let i = 0, l = keys.length; i < l; i++) {
        const keyBuffer = Buffer.from(keys[i], 'utf8')
        const valueBuffer = txn.getBinary(this.dbi, keyBuffer)
        results[i] = (valueBuffer != null) ? unpackr.unpack(valueBuffer) : null
      }
    }, true)
    return callback ? callback(null, results) : results
  }

  backup(dest, compact = false) {
    mkdirSync(dest, { recursive: true })
    return new Promise((resolve, reject) => this.env.copy(dest, compact, (error) => {
      if (error) {
        console.error(`error attempting copy`, error)
        reject(error)
      } else {
        resolve()
      }
    }))
  }

  getCount() {
    return this.transact((txn) => {
      return this.dbi.stat(txn)?.entryCount
    })
  }

  close() {
    this.dbi.close()
    this.env.close()
  }
}

module.exports = { Store, asBinary }

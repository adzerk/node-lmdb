const { pack, unpack } = require('msgpackr')
const { mkdirSync } = require('fs')
const { Cursor, Env } = require('node-gyp-build')(__dirname)

function asBinary(buffer) {
  return {
    ['\x10binary-data\x02']: buffer,
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
              value: unpack(self.cursor.getCurrentBinary())
            }
          } else if (keys) {
            value = key.toString('utf8')
          } else if (values) {
            value = unpack(self.cursor.getCurrentBinary())
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
 * This class enforces a very specific usage of lmdb. All keys are utf8 encoded
 * buffers and all values are encoded with msgpack. Transaction handling is also
 * intentionally simplified to just one current transaction, no nesting, with
 * the assumption that there is only one active client. This is the pattern that
 * adset-consumer uses in its current form.
 */
class Store {
  // TODO: add in support for a persistent readonly transaction for the lifetime
  // of the Store instance - Engines will need this
  // TODO: maybe refactor to split env/dbi params
  // TODO: support additional dbis
  constructor({
    create = false,
    mapSize,
    name,
    noReadAhead = false,
    path
  }) {
    this.env = new Env()
    this.env.open({ path, mapSize, noReadAhead })
    this.dbi = this.env.openDbi({ name, create, keyIsBuffer: true })
    this.txn = null
  }

  put(key, value) {
    this.transact(() => {
      try {
        const keyBuffer = Buffer.from(key, 'utf8')
        let valueBuffer
        if (value && value['\x10binary-data\x02'])
          valueBuffer = value['\x10binary-data\x02']
        else
          valueBuffer = pack(value)
        this.txn.putBinary(this.dbi, keyBuffer, valueBuffer)
      } catch (error) {
        console.error('Error storing value:', error)
      }
    })
  }

  get(key) {
    return this.transact(() => {
      try {
        const keyBuffer = Buffer.from(key, 'utf8')
        const value = this.txn.getBinary(this.dbi, keyBuffer)
        return value == null ? null : unpack(value)
      } catch (error) {
        console.error('Error retrieving value:', error)
        return null
      }
    }, true)
  }

  del(key) {
    return this.transact(() => {
      try {
        const keyBuffer = Buffer.from(key, 'utf8')
        if (this.txn.getBinary(this.dbi, keyBuffer) != null) {
          this.txn.del(this.dbi, keyBuffer)
          return true
        } else {
          return false
        }
      } catch (error) {
        console.error('Error deleting value:', error)
      }
    })
  }

  /**
   * Wrapper function for a transaction. This is only sensible in adset-consumer
   * as there is only ever one thread of control working on the Store instance
   * at a time.
   * @param {*} f
   */
  transact(f, readonly = false) {
    let ownTxn = false
    if (!this.txn) {
      this.txn = this.env.beginTxn()
      ownTxn = true
    }

    try {
      const result = f()
      if (ownTxn) {
        if (readonly) {
          this.txn.abort()
        }
        else {
          this.txn.commit()
        }
      }
      return result
    } catch (error) {
      console.error('Transaction aborted due to an error:', error.message)
      this.txn.abort()
      throw error
    } finally {
      if (ownTxn)
        this.txn = null
    }
  }

  async transactAsync(f, readonly = false) {
    let ownTxn = false
    if (!this.txn) {
      this.txn = this.env.beginTxn()
      ownTxn = true
    }

    try {
      const result = await f()
      if (ownTxn) {
        if (readonly) {
          this.txn.abort()
        }
        else {
          this.txn.commit()
        }
      }
      return result
    } catch (error) {
      console.error('Transaction aborted due to an error:', error.message)
      this.txn.abort()
      throw error
    } finally {
      if (ownTxn)
        this.txn = null
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
    this.transact(() => {
      for (let i = 0, l = keys.length; i < l; i++) {
        const keyBuffer = Buffer.from(keys[i], 'utf8')
        const valueBuffer = this.txn.getBinary(this.dbi, keyBuffer)
        results[i] = (valueBuffer != null) ? unpack(valueBuffer) : null
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
    return this.transact(() => {
      return this.dbi.stat(this.txn)?.entryCount
    })
  }

  close() {
    this.dbi.close()
    this.env.close()
  }
}

module.exports = { Store, asBinary }

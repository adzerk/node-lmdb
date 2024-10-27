const { assert } = require('chai');
const fs = require('fs')
const { pack } = require ('msgpackr')
const { Store, asBinary } = require('../store.js')

describe('Store class', function () {
  const testDbPath = './testdb'
  let store

  beforeEach(() => {
    if (fs.existsSync(testDbPath)) {
      fs.rmSync(testDbPath, { recursive: true })
    }
    fs.mkdirSync(testDbPath, { recursive: true })
    store = new Store({ name: 'testdb', path: testDbPath, create: true })
  })

  afterEach(() => {
    store.close()
    if (fs.existsSync(testDbPath)) {
      fs.rmSync(testDbPath, { recursive: true })
    }
  })

  describe('put and get methods', function () {
    it('should store and retrieve a value from the database', function () {
      const key = 'user1'
      const value = { name: 'Alice', age: 25 }

      store.put(key, value)
      const retrievedValue = store.get(key)

      assert.deepEqual(retrievedValue, value)
    })

    it('should store and retrieve binary data correctly', function () {
      const key = 'binaryKey'
      const value = {some: 'value', another: 'one'}
      const buffer = pack(value)

      store.put(key, asBinary(buffer))
      const retrievedValue = store.get(key)

      assert.deepEqual(retrievedValue, value)
    })

    it('should return null for non-existent keys', function () {
      const nonExistentKey = 'nonExistent'
      const result = store.get(nonExistentKey)

      assert.isNull(result)
    })
  })

  describe('del method', function () {
    it('should delete a value from the database', function () {
      const key = 'user1'
      const value = { name: 'Alice', age: 25 }

      store.put(key, value)
      const success = store.del(key)
      const result = store.get(key)

      assert.isTrue(success)
      assert.isNull(result)
    })
  })

  describe('transact method', function () {
    it('should successfully commit a transaction', function () {
      const key = 'user1'
      const value = { name: 'Alice', age: 25 }

      store.transact(() => {
        store.put(key, value)
      })
      const retrievedValue = store.get(key)

      assert.deepEqual(retrievedValue, value)
    })

    it('should abort a transaction on error', function () {
      const key = 'user1'
      const value = { name: 'Alice', age: 25 }

      try {
        store.transact(() => {
          store.put(key, value)
          throw new Error('Test error') // Simulate an error
        })
      } catch (e) {}
      const result = store.get(key)

      assert.isNull(result)
    })
  })

  describe('iterate method', function () {
    it('should iterate over all key-value pairs', function () {
      const key1 = 'user1'
      const key2 = 'user2'
      const value1 = { name: 'Alice', age: 25 }
      const value2 = { name: 'Bob', age: 30 }

      store.put(key1, value1)
      store.put(key2, value2)
      const results = []

      for (const { key, value } of store.iterate()) {
        results.push({ key, value })
      }

      assert.deepEqual(results, [
        { key: 'user1', value: value1 },
        { key: 'user2', value: value2 },
      ])
    })
  })

  describe('getCount method', function () {
    it('should correctly report the number of keys', function () {
      const key1 = 'user1'
      const key2 = 'user2'
      const value1 = { name: 'Alice', age: 25 }
      const value2 = { name: 'Bob', age: 30 }

      store.put(key1, value1)
      store.put(key2, value2)

      const result = store.getCount()
      assert.deepEqual(result, 2)
    })
  })
})

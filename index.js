'use strict';

const store = require('./store.js')

module.exports = {
  ...require('node-gyp-build')(__dirname),
  ...store
}

main = require './main.js'
assert = require('assert')

assert.ok([] == main.main([]), "Test failed")

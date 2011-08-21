fs = require 'fs'
assert = require('assert')

print = (msg) ->
  console.log msg
exports.print = print
  
printAssert = (expected, actual, result, passMsg, failMsg) ->
  if result
    print passMsg
  else
    print "EXPECTED #{expected} != ACTUAL #{actual}.  result == #{result}\n"
    assert.ok(result, failMsg)
exports.printAssert = printAssert

debugLogFile = 'debug_log.txt'
debugLog = (msg) ->
  fs = require 'fs'
  fs.writeFile debugLogFile, msg, (err) ->
    throw err if err
exports.debugLog = debugLog
exports.debugLogFile = debugLogFile


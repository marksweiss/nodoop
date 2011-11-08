fs = require 'fs'
assert = require('assert')
# Dummy comment

print = (msg) ->
  console.log msg
exports.print = print
  
printAssert = (expected, actual, result, passMsg, failMsg) ->
  if result
    print passMsg
  else
    print "EXPECTED\n#{expected}\n!= ACTUAL\n#{actual}\nresult == #{result}\n"
    assert.ok(result, failMsg)
exports.printAssert = printAssert

printErr = (err) ->
  print("exec error:\n" + err + "\n") 
  print 'error code: ' + err.code + "\n"
  print 'error signal: ' + err.signal + "\n"
  print "stderr: \n" + process.stderr
exports.printErr = printErr

debugLogFile = 'debug_log.txt'
debugLog = (msg) ->
  fs = require 'fs'
  fs.writeFile debugLogFile, msg, (err) ->
    throw err if err
exports.debugLog = debugLog
exports.debugLogFile = debugLogFile

isString = (obj) ->
	typeof obj == 'string'
exports.isString = isString

isUndefined = (obj) ->
	typeof obj == 'undefined'
exports.isUndefined = isUndefined
main = require './main.js'
U = require './util.js'
conf = require('./config.js').config
assert = require('assert')
exec = require('child_process').exec
  
# Tests

test_getCli = () ->
  testName = 'test_getCli'
  
  inFilePath = 'Users/admin/Sites/nodoop/in'
  outFilePath = 'Users/admin/Sites/nodoop/out'
  mapper = 'Users/admin/Sites/nodoop/map/map.js'
  reducer = 'Users/admin/Sites/nodoop/map/reducer.js'
  args = [inFilePath, outFilePath, mapper, reducer]

  expected = "#{conf.hadoopBinPath}/hadoop jar #{conf.hadoopHome}/contrib/streaming/hadoop-*-streaming.jar \\\n
-input #{inFilePath} \\\n
-output #{outFilePath} \\\n
-mapper #{mapper} \\\n
-reducer #{reducer}"
  actual = main.getCli args  
  result = (expected == actual)
  
  U.printAssert(expected, actual, result, "#{testName} passed", "#{testName} failed")
  assert.ok(result, '')
#
test_getCli()

test_mainRunIt = () ->
  testName = 'test_mainRunIt'
 
  inFilePath = 'Users/admin/Sites/nodoop/test/in'
  outFilePath = 'Users/admin/Sites/nodoop/test/out'
  mapper = 'node Users/admin/Sites/nodoop/test/map/map.js'
  reducer = 'node Users/admin/Sites/nodoop/test/reduce/reduce.js'
  args = [inFilePath, outFilePath, mapper, reducer]
  cli = main.getCli args
 
  child = exec cli, (error, stdout, stderr) ->
    if error?
      U.print('exec error: ' + error + "\n") 
      U.print 'error code: ' + error.code + "\n"
      U.print 'error signal: ' + error.signal + "\n"
      U.print "stderr: \n" + stderr
    else
      U.print stdout
  
  # U.printAssert(expected, actual, result, "#{testName} passed", "#{testName} failed")
  # assert.ok(result, '')
#
test_mainRunIt()

main = require './main.js'
U = require './util.js'
conf = require('./config.js').config
assert = require('assert')
exec = require('child_process').exec
  
# Tests

test_getCli = () ->
  testName = 'test_getCli'
  
  inFilePath = '/Users/admin/Sites/nodoop/test/in'
  outFilePath = '/Users/admin/Sites/nodoop/test/out'
  mapper = '/Users/admin/Sites/nodoop/test/map/map.js'
  reducer = '/Users/admin/Sites/nodoop/test/reduce/reduce.js'
  args = [inFilePath, outFilePath, mapper, reducer]

  expected = "#{conf.hadoopBinPath}/hadoop jar #{conf.hadoopStreamingJar} \\\n
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
 
  inFilePath = '/Users/admin/Sites/nodoop/test/in'
  outFilePath = '/Users/admin/Sites/nodoop/test/out'
  mapper = '/Users/admin/Sites/nodoop/test/map/map.js'
  reducer = '/Users/admin/Sites/nodoop/test/reduce/reduce.js'
  args = [inFilePath, outFilePath, mapper, reducer]
  cli = main.getCli args
 
  # TODO FAILING NEXT THING TO FIX IS
  # HashBang node line at top of map and reduce not being preserved by coffee compiler
  # If no answer conver these to JavaScript for now and open an issue to fix it

  # marksweiss: question: is there a way to pass through a shebang line at the top of a .coffee file into the .js output?
  # [09:40am] esparkma_ joined the chat room.
  # [09:41am] esparkman left the chat room. (Read error: No route to host)
  # [09:41am] TheJH: marksweiss, there are a quick way and a clean way, I think
  # [09:42am] TheJH: marksweiss, the quick way only works if you use the -b option and is that you put this on the first line: `#! /whatever
  # [09:42am] TheJH: the second whatever-it-is-called-thing (`) comes on the second line
  # [09:44am] marksweiss: OK thanks! I'll look at thtat
 
 
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

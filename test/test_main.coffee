`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

main = require '../main.js'
U = require '../util/util.js'
conf = require('../config.js').config
Mapper = require('../map.js').Mapper
assert = require('assert')
exec = require('child_process').exec
fs = require 'fs'

# Tests
test_getCl = () ->
  testName = 'test_getCl'
  
  inFilePath = '/hdfs/Users/admin/Sites/nodoop/test/in/testin.txt'
  outFilePath = '/hdfs/Users/admin/Sites/nodoop/test/out'
  mapper = '/Users/admin/Sites/nodoop/test/map/map.js'
  reducer = '/Users/admin/Sites/nodoop/test/reduce/reduce.js'
  args = [inFilePath, outFilePath, mapper, reducer]

  expected = "#{conf.hadoopBinPath}/hadoop jar #{conf.hadoopStreamingJar} \\\n
-input #{inFilePath} \\\n
-output #{outFilePath} \\\n
-mapper #{mapper} \\\n
-reducer #{reducer} \\\n
-file #{mapper} \\\n
-file #{reducer}"
  actual = main.getCl args  
  result = (expected == actual)
  
  U.printAssert(expected, actual, result, "#{testName} passed", "#{testName} failed")
  return
#
test_getCl()

test_map = () ->
  testName = 'test_map'  
  outFile = '../out/test_map_out.txt'
  inStrm = fs.createReadStream '../in/testin.txt', encoding: 'utf8'
  outStrm = fs.createWriteStream outFile, {encoding: 'utf8', flag: 'a'} # 'a' mode appends writes  
    
  # Implement map operation on each line of data, using whatever logic you want and
  #  outputting to whatever output stream passed to the Mapper constructor.
  # Note that really streaming against Hadoop requires writing to stdout, which is the default in Mapper ctor
  dataCb = (data, outStrm) ->
    tkns = data.split(///\s+///)
    for tkn in tkns
      outStrm.write tkn.trim() + "\t\n", encoding='utf8'
    return
  
  # Collect and test results
  endCb = (outStrm) ->
    expected = "hello\t\nworld\t\nhello\t\ngoodbye\t\nI\t\nam\t\na\t\nchicken"    

    # Handle the 'drain' event, which fires after writing to stream completed
    outStrm.on 'drain', ->
      # Write EOF to the file so that #fs.readFileSync() can read from it
      # Note that destroying the stream here led to errors. Don't know why.
      outStrm.end()
      # Set actual here in scope of handler, where we know outStrm is done writing to and we can read the file it wrote 
      actual = (fs.readFileSync outFile, encoding='utf8').trim()
      # Test expected == actual here in handler scope, because otherwise actual falls out of scope
      result = (expected == actual)
      U.printAssert(expected, actual, result, "#{testName} passed", "#{testName} failed")
      return

    outStrm.on 'error', (error) ->
      U.print 'ERROR'
      # Print this way because concatting on one line prints a less complete string repr of the error object
      U.print error
      return

    return  
  
  mapper = new Mapper(inStrm, outStrm, dataCb, endCb)
  mapper.map()
  return

test_map()

test_mainRunIt = () ->
  testName = 'test_mainRunIt'
 
  inFilePath = '/hdfs/Users/admin/Sites/nodoop/test/in/testin.txt'
  outFilePath = '/hdfs/Users/admin/Sites/nodoop/test/out'
  mapper = '/Users/admin/Sites/nodoop/test/map/map.js'
  reducer = '/Users/admin/Sites/nodoop/test/reduce/reduce.js'
  args = [inFilePath, outFilePath, mapper, reducer]
  cl = main.getCl args
  
  child = exec cl, (error, stdout, stderr) ->
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
# test_mainRunIt()

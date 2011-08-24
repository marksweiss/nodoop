`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

main = require '../main.js'
U = require '../util/util.js'
conf = require('../config.js').config
Mapper = require('../map.js').Mapper
StructuredMapper = require('../map.js').StructuredMapper
HadoopMapper = require('../map.js').HadoopMapper
Reducer = require('../reduce.js').Reducer

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

test_mapper_map = () ->
  testName = 'test_mapper_map'  
  outFile = '../out/test_map_out.txt'
  inStrm = fs.createReadStream '../in/testin.txt', encoding: 'utf8'
  outStrm = fs.createWriteStream outFile, {encoding: 'utf8', flag: 'a'} # 'a' mode appends writes  
    
  # Implement map operation on each line of data, using whatever logic you want and
  #  outputting to whatever output stream passed to the Mapper constructor.
  # Note that really streaming against Hadoop requires writing to stdout, which is the default in Mapper ctor
  dataCb = (data, outStrm) ->
    tkns = data.split(///\s+///)
    for tkn in tkns
      tkn = tkn.trim()
      # Concatting tab here is manually delimiting key and rest of line in Hadoop streaming convention
      outStrm.write(tkn + "\t\n", encoding='utf8') if tkn.length > 0
    return
  
  # Collect and test results
  endCb = (inStrm, outStrm) ->
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
  
  mapper = new Mapper(dataCb, endCb, inStrm, outStrm)
  mapper.map()
  return

test_mapper_map_key_and_rest = () ->
  testName = 'test_mapper_map_key_and_rest'  
  outFile = '../out/test_map_out2.txt'
  inStrm = fs.createReadStream '../in/testin2.txt', encoding: 'utf8'
  outStrm = fs.createWriteStream outFile, {encoding: 'utf8', flag: 'a'} # 'a' mode appends writes  
  
  # CB gets line of data, outputStrm passed to Mapper, and two helpers mapper,
  #  one for splitting input line into key and rest, other for building output line from key and rest
  dataCb = (data, outStrm, splitLineCb, buildLineCb) ->
    # Silly mock helper something like what you'd use with structured data
    areaCodeToCity = (phoneNum) ->
      return 'Manhattan' if phoneNum[0..2] == '212'
      return 'Unknown'
    
    # Split the line into array of key string and rest of the line string
    # Non-Structured Mapper splits on Hadoop streaming default to delimit the key, which is tab
    [key, rest] = splitLineCb data
    # Client has to know how to parse rest of the line with unstructured mapper    
    rest = rest.split('|')
    # Derive and append an additional value and it to the fields in rest
    phoneNum = rest[1]
    city = areaCodeToCity phoneNum
    rest.push city
    # Convert fields back to delimited string with same | delimiter
    rest = rest.join('|')
    # Build line using the helper provided by Mapper to this callback
    line = buildLineCb(key, rest)
    outStrm.write(line)
    return
  
  # Collect and test results
  endCb = (inStrm, outStrm) ->
    expected = "123\tSnippy Jackson|212-555-1212|45|Manhattan\n456\tWanda Jenkins|212-555-1313|29|Manhattan\n789\tChuck Norris|718-555-1414|58|Unknown"    

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
  
  mapper = new Mapper(dataCb, endCb, inStrm, outStrm)
  mapper.map()
  return

# TODO THIS NO LONGER MAKES SENSE AS A USE CASE?

# test_structured_mapper_map = () ->
#   testName = 'test_structured_mapper_map'  
#   outFile = '../out/test_map_out3.txt'
#   inStrm = fs.createReadStream '../in/testin3.txt', encoding: 'utf8'
#   outStrm = fs.createWriteStream outFile, {encoding: 'utf8', flag: 'a'} # 'a' mode appends writes  
#     
#   dataCb = (data, outStrm) ->
#     delim = ' '
#     tkns = splitLineCb(data, delim)
#     for tkn in data
#       tkn = tkn.trim()      
#       outStrm.write tkn.toString() + "\t\n", encoding='utf8'
#     return
#   
#   # Collect and test results
#   endCb = (inStrm, outStrm) ->
#     expected = "hello\t\nworld\t\nhello\t\ngoodbye\t\nI\t\nam\t\na\t\nchicken"    
# 
#     # Handle the 'drain' event, which fires after writing to stream completed
#     outStrm.on 'drain', ->
#       # Write EOF to the file so that #fs.readFileSync() can read from it
#       # Note that destroying the stream here led to errors. Don't know why.
#       outStrm.end()
#       # Set actual here in scope of handler, where we know outStrm is done writing to and we can read the file it wrote 
#       actual = (fs.readFileSync outFile, encoding='utf8').trim()
#       # Test expected == actual here in handler scope, because otherwise actual falls out of scope
#       result = (expected == actual)      
#       U.printAssert(expected, actual, result, "#{testName} passed", "#{testName} failed")
#       return
# 
#     outStrm.on 'error', (error) ->
#       U.print 'ERROR'
#       # Print this way because concatting on one line prints a less complete string repr of the error object
#       U.print error
#       return
# 
#     return  
#   
#   mapper = new StructuredMapper(dataCb, endCb, inStrm, outStrm)
#   mapper.map()
#   return

test_structured_mapper_map_key_and_rest = () ->
  testName = 'test_structured_mapper_map_key_and_rest'  
  outFile = '../out/test_map_out4.txt'
  inStrm = fs.createReadStream '../in/testin4.txt', encoding: 'utf8'
  outStrm = fs.createWriteStream outFile, {encoding: 'utf8', flag: 'a'} # 'a' mode appends writes  
  delim = '|'
  
  # This version of the Mapper provides data as an array of trimmed tokens split on delim
  dataCb = (data, outStrm, splitLineCb, buildLineCb) ->
    # Silly mock helper something like what you'd use with structured data
    areaCodeToCity = (phoneNum) ->
      return 'Manhattan' if phoneNum[0..2] == '212'
      return 'Unknown'
    
    # Tell the callback being used to partition the fields in the row, which indexes are part of the key
    keyIndexes = [0]
    delim = '|'
    # Return from StructuredMapper is an arra of two arrays, 
    #  first is values from key fields in row, second is values from rest of fields in row
    [key, rest] = splitLineCb(data, keyIndexes, delim)            
    # Derive and append an additional value and it to the fields in rest
    phoneNum = rest[1]
    city = areaCodeToCity phoneNum
    rest.push city
    #
    outStrm.write buildLineCb(key, rest, delim), encoding='utf8'
    return
  
  # Collect and test results
  endCb = (inStrm, outStrm) ->
    expected = "123\tSnippy Jackson|212-555-1212|45|Manhattan\n456\tWanda Jenkins|212-555-1313|29|Manhattan\n789\tChuck Norris|718-555-1414|58|Unknown"    

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
  
  mapper = new StructuredMapper(dataCb, endCb, inStrm, outStrm)
  mapper.map(delim)
  return

# NOTE!!!!! GIANT TODO HERE
# TODO To make reduce work, we need to append sentinel value as last line
#  of input. Need to hide this in the plumbing that copies files into HDFS
#  by having that code append sentinel values, which maps ignore but reduces rely on
test_reducer_reduce = () ->
  testName = 'test_reducer_reduce'
  outFile = '../out/test_map_out5.txt'
  # NOTE TODO THIS IS THE FILE WITH THE DUMMY SENTINEL VALUE
  inStrm = fs.createReadStream '../in/testin5.txt', encoding: 'utf8'
  outStrm = fs.createWriteStream outFile, {encoding: 'utf8', flag: 'a'} # 'a' mode appends writes  
  
  # Simplest possible word count implementation. Run counter while key is the same
  #  and when it flips to the next one record a row of last key just passed
  #  and accumulated count and reset counter
  count = 1
  dataSameKeyCb = (key, rest, outStrm, buildLineCb) -> 
    count += 1
  dataNewKeyCb = (lastKey, rest, outStrm, buildLineCb) ->    
    outStrm.write buildLineCb(lastKey, count + '')
    count = 1

  # Collect and test results
  endCb = (inStrm, outStrm) ->
    expected = "a\t1\nam\t1\nchicken\t1\ngoodbye\t1\nhello\t2\nI\t1\nworld\t1"

    # Handle the 'drain' event, which fires after writing to stream completed
    outStrm.on 'drain', ->
      outStrm.end()
      actual = (fs.readFileSync outFile, encoding='utf8').trim()
      result = (expected == actual)      
      U.printAssert(expected, actual, result, "#{testName} passed", "#{testName} failed")
      return

    outStrm.on 'error', (error) ->
      U.print 'ERROR'
      U.print error
      return

    return  
  
  reducer = new Reducer(dataSameKeyCb, dataNewKeyCb, endCb, inStrm, outStrm)
  reducer.reduce()
  return

test_structured_reducer_reduce = () ->
  testName = 'test_structured_reducer_reduce'
  outFile = '../out/test_map_out6.txt'
  # NOTE TODO THIS IS THE FILE WITH THE DUMMY SENTINEL VALUE
  inStrm = fs.createReadStream '../in/testin6.txt', encoding: 'utf8'
  outStrm = fs.createWriteStream outFile, {encoding: 'utf8', flag: 'a'} # 'a' mode appends writes  
  delim = '|'

  # Simplest possible word count implementation. Run counter while key is the same
  #  and when it flips to the next one record a row of last key just passed
  #  and accumulated count and reset counter
  count = 1
  dataSameKeyCb = (key, rest, outStrm, buildLineCb) -> 
    count += 1
  dataNewKeyCb = (lastKey, rest, outStrm, buildLineCb) ->    
    outStrm.write buildLineCb(lastKey, count + '', delim)
    count = 1

  # Collect and test results
  endCb = (inStrm, outStrm) ->
    expected = "a\t1\nam\t1\nchicken\t1\ngoodbye\t1\nhello\t2\nI\t1\nworld\t1"
    
    # Handle the 'drain' event, which fires after writing to stream completed
    outStrm.on 'drain', ->
      outStrm.end()
      actual = (fs.readFileSync outFile, encoding='utf8').trim()
      result = (expected == actual)      
      U.printAssert(expected, actual, result, "#{testName} passed", "#{testName} failed")
      return

    outStrm.on 'error', (error) ->
      U.print 'ERROR'
      U.print error
      return

    return  

  reducer = new Reducer(dataSameKeyCb, dataNewKeyCb, endCb, inStrm, outStrm)
  reducer.reduce(delim)
  return

# Integration test that tests HadoopMapper and integration with Hadoop streaming
test_mainRunIt = () ->
  testName = 'test_mainRunIt'
 
  inFilePath = '/hdfs/Users/admin/Sites/nodoop/test/in/testin.txt'
  outFilePath = '/hdfs/Users/admin/Sites/nodoop/test/out'
  mapper = '/Users/admin/Dropbox/projects/nodoop/js/test/test_map.js'
  reducer = '/Users/admin/Dropbox/projects/nodoop/js/test/test_reduce.js'
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


runTests = ->
  test_getCl()
  test_mapper_map()
  test_mapper_map_key_and_rest()
  test_structured_mapper_map_key_and_rest()  
  test_reducer_reduce()
  test_structured_reducer_reduce()
  test_mainRunIt()
  
runTests()
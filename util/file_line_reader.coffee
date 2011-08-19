fs = require 'fs'

# Adapted directly from here: http://blog.jaeckel.com/2010/03/i-tried-to-find-example-on-using-node.html
exports = class FileLineReader
  constructor: (@filename, @bufferSize = 8192) ->
    @buffer = ''
    @fd = fs.openSync(@filename, 'r')
    @curPos = @fillBuffer(0)

  fillBuffer: (pos) ->
    res = fs.readSync(@fd, @bufferSize, pos)
    data = res[0]
    offset = res[1]
        
    @buffer += data
    if offset isnt 0
      pos + offset
    else
      -1
    
  hasNextLine: ->
    while @buffer.indexOf("\n") is -1
      @curPos = @fillBuffer(@curPos)
      return false if @curPos is -1 
    if @buffer.indexOf("\n") isnt -1
      true
    else
      false

  nextLine: ->
    lineEnd = @buffer.indexOf "\n"
    result = @buffer.substring(0, lineEnd)
    @buffer = @buffer.substring(result.length + 1, buffer.length)
    result

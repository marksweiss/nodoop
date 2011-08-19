`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

Lazy = require 'lazy'
U = require '../../util/util.js'
EventEmitter = require('events').EventEmitter

class Mapper extends EventEmitter
  constructor: (outStrm = process.stdout) -> 
    @outStrm = outStrm
    @finalStr = ''  # Just a memory location so client has a string in scope in 'end' event handler
    super()
  
  map: (inStrm) ->
    line = ''  
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    new Lazy(inStrm).forEach (line) =>
      @emit 'data', line, @outStrm
    @emit 'end', @finalStr, @outStrm
    return  # Coffee hack to generate JS function with no return value

# TOP LEVEL WRAPPER FOR HADOOP STREAMING
map = ->
  mapper = new Mapper()
  mapper.on 'data', (data, outStrm) -> # YOUR MAP HANDLER HERE
  mapper.on 'end', (finalStr, outStrm) -> process.stdin.resume()    
  mapper.map(process.stdin)
  return

# Export the class to allow construction with or without outStrm arg
exports.Mapper = Mapper
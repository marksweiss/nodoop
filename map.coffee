`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

Lazy = require 'lazy'
U = require './util/util.js'
EventEmitter = require('events').EventEmitter

class MapperBase extends EventEmitter
  constructor: (inStrm, outStrm, dataCb, endCb) ->
    @inStrm = inStrm
    @outStrm = outStrm
    @dataCb = dataCb
    @endCb = endCb
    super()
  
  map: ->
    line = ''
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    new Lazy(@inStrm).forEach (line) =>      
      @emit 'data', line, @outStrm, @dataCb
    @emit 'end', @outStrm, @endCb
    return

# Separate parent class to have mapper object and handle data and end events on it
# Also set defauls here. May come in handy to extend the design here.
class Mapper
  constructor: (inStrm, outStrm, dataCb, endCb) ->
    # Default values will create pass through mapper from stdin to stdout
    inStrm ?= process.stdin
    outStrm ?= process.stdout
    dataCb = dataCb ?= ( (data, outStrm) -> outStrm.write(data) )
    endCb = endCb ?= ( () -> inStrm.resume() )
    @mapper = new MapperBase(inStrm, outStrm, dataCb, endCb)

  map: ->
    @mapper.on 'data', (data, outStrm, dataCb) -> dataCb(data, outStrm)
    @mapper.on 'end', (outStrm, endCb) -> endCb(outStrm)
    @mapper.map()

# Export the class to allow construction with or without outStrm arg
exports.Mapper = Mapper
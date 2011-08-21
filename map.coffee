`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

# TODO FILE COFFEESCRIPT BUG IF NOT ALREADY OPEN
# coffee> y.trim() for y in x
# [ 'a', 'b' ]
# coffee> x = y.trim() for y in x
# TypeError: Cannot call method 'trim' of undefined    

Lazy = require 'lazy'
U = require './util/util.js'
EventEmitter = require('events').EventEmitter

class MapperBase extends EventEmitter
  constructor: (dataCb, endCb, inStrm, outStrm) ->
    @dataCb = dataCb
    @endCb = endCb
    @inStrm = inStrm
    @outStrm = outStrm
    super()
  
  map: ->
    line = ''
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    new Lazy(@inStrm).lines.map(String).map (line) =>     
      @emit 'data', line, @outStrm, @dataCb
    @emit 'end', @inStrm, @outStrm, @endCb
    return

# Identical structure to Mapper but presents dataCb with an Array, not String
#  by splitting each line with the delimiter provided
class StructuredMapperBase extends EventEmitter
  constructor: (dataCb, delim, endCb, inStrm, outStrm) ->
    @dataCb = dataCb
    @endCb = endCb
    @inStrm = inStrm
    @outStrm = outStrm
    @delim = delim
    super() 

  map: ->
    line = ''
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    new Lazy(@inStrm).lines.map(String).map (line) =>
      line = line.trim()
      tkns = []
      tkns.push((tkn = tkn.trim())) for tkn in line.split(@delim) when tkn.length > 0
      @emit 'data', tkns, @outStrm, @dataCb
    
    @emit 'end', @inStrm, @outStrm, @endCb
    return

# Separate parent class to have mapper object and handle data and end events on it
# Also set defauls here. May come in handy to extend the design here.
class Mapper
  constructor: (dataCb = null, endCb = null, inStrm = null, outStrm = null) ->
    # Default values will create pass through mapper from stdin to stdout
    inStrm or= process.stdin
    outStrm or= process.stdout
    dataCb or= (data, outStrm) -> outStrm.write data
    endCb or= (inStrm, outStrm) -> inStrm.resume()
    @mapper = new MapperBase(dataCb, endCb, inStrm, outStrm)

  map: ->
    @mapper.on 'data', (data, outStrm, dataCb) -> dataCb(data, outStrm)
    @mapper.on 'end', (inStrm, outStrm, endCb) -> endCb(inStrm, outStrm)
    @mapper.map()

class StructuredMapper
  constructor: (dataCb = null, delim = ' ', endCb = null, inStrm = null, outStrm = null) ->
    # Default values will create pass through mapper from stdin to stdout
    inStrm or= process.stdin
    outStrm or= process.stdout
    dataCb or= (data, outStrm) -> outStrm.write data
    endCb or= (inStrm, outStrm) -> inStrm.resume()
    @mapper = new StructuredMapperBase(dataCb, delim, endCb, inStrm, outStrm)

  map: ->
    @mapper.on 'data', (data, outStrm, dataCb) -> dataCb(data, outStrm)
    @mapper.on 'end', (inStrm, outStrm, endCb) -> endCb(inStrm, outStrm)
    @mapper.map()

# Wrapper to present an interface that only needs a data callback
#  to process each row, and uses defaults for stdin, stdout, and no-op end callback
# This is the minimum default necessary for Hadoop streaming.
class HadoopMapper extends Mapper
  constructor: (dataCb) ->
    super(dataCb)

class HadoopStructuredMapper extends StructuredMapper
  constructor: (dataCb, delim = ' ') ->
    super(dataCb, delim)
  
# Export the class to allow construction with or without outStrm arg
exports.Mapper = Mapper
exports.HadoopMapper = HadoopMapper
exports.StructuredMapper = StructuredMapper
exports.HadoopStructuredMapper = HadoopStructuredMapper
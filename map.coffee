`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

# TODO FILE COFFEESCRIPT BUG IF NOT ALREADY OPEN
# coffee> y.trim() for y in x
# [ 'a', 'b' ]
# coffee> x = y.trim() for y in x
# TypeError: Cannot call method 'trim' of undefined

# ANOTHER LIST COMPREHENSION BUG
# FAILS: if i in keyIndexes then key.push rec[i] else rest.push rec[i] for i in [0..rec.length-1]
# SUCCEEDS:
# for i in [0..rec.length-1]
#   if i in keyIndexes
#     key.push rec[i] 
#   else
#     rest.push rec[i]    

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
      @emit 'data', line, @outStrm, @dataCb, @splitKeyAndRest, @buildKeyAndRest
    @emit 'end', @inStrm, @outStrm, @endCb
    return

  # Returns an array of the key part of the line and the rest of the line
  splitKeyAndRest: (line) -> 
    ret = line.trim().split("\t")
    ret = ['', ''] if U.isUndefined(ret) or ret.length == 0
    # If split only brought back stuff to left of delim, make that key and append empty rest
    ret.push '' if ret.length == 1
    ret
    
  # Builds a string with the key and the rest of the line delimited by tab
  buildKeyAndRest: (key, rest) -> key + "\t" + rest + "\n"


# Identical structure to Mapper but presents dataCb with an Array, not String
#  by splitting each line with the delimiter provided
class StructuredMapperBase extends EventEmitter
  constructor: (dataCb, endCb, inStrm, outStrm) ->
    @dataCb = dataCb
    @endCb = endCb
    @inStrm = inStrm
    @outStrm = outStrm
    super() 

  map: (delim) ->
    line = ''
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    new Lazy(@inStrm).lines.map(String).map (line) =>
      line = line.trim()
      tkns = []      
      tkns.push((tkn = tkn.trim())) for tkn in line.split(delim) when tkn.length > 0
      @emit 'data', tkns, @outStrm, @dataCb, @splitKeyAndRest, @buildKeyAndRest
    @emit 'end', @inStrm, @outStrm, @endCb
    return

  splitKeyAndRest: (rec, keyIndexes) ->
    ret = []
    key = []
    rest = []
    # If the client is using object in key/rest mode, i.e. if they provided keyIdxs
    #  then walk the array of tkns from the line and group into key and rest.
    # OTOH, if there aren't key indexes, this code puts everything into key and nothing into rest
    if keyIndexes? and keyIndexes.length > 0
      # TODO - this is O(N*M), even with the pretty 'in' syntax obscuring the inner loop
      for i in [0..rec.length-1]
        if i in keyIndexes
          key.push rec[i] 
        else
          rest.push rec[i] 
    else
      key[i] = rec[i] for i in [0..rec.length-1]

    ret.push key
    ret.push rest
    ret
  
  # Now stringify because building output to write to outStrm
  buildKeyAndRest: (key, rest, delim) -> 
    kOut = []
    rOut = []
    kOut.push k for k in key
    rOut.push r for r in rest
    kOut.join(delim) + "\t" + rOut.join(delim) + "\n" 


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
    @mapper.on 'data', (data, outStrm, dataCb, splitLineCb, buildLineCb) -> 
      dataCb(data, outStrm, splitLineCb, buildLineCb)
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

  map: (delim) ->
    @mapper.on 'data', (data, outStrm, dataCb, splitLineCb, buildLineCb) -> 
      dataCb(data, outStrm, splitLineCb, buildLineCb)
    @mapper.on 'end', (inStrm, outStrm, endCb) -> endCb(inStrm, outStrm)
    @mapper.map(delim)

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
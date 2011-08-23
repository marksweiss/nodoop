`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

Lazy = require 'lazy'
U = require './util/util.js'
EventEmitter = require('events').EventEmitter

# lastWord = ''
# wordTotal = 0
# new Lazy(process.stdin).forEach (line) -> 
#   word = line.split("\t")[0]
#   if lastWord isnt word
#     U.print(lastWord + "\t" + wordTotal)  # Maps operate by side-effect, namely printing lines of output to stdout
#     lastWord = word
#     wordTotal = 1
#   else
#     wordTotal += 1
#   return  # Coffee hack to generate JS function with no return value
#          
# process.stdin.resume()

class ReducerBase extends EventEmitter
  constructor: (dataSameKeyCb, dataNewKeyCb, endCb, inStrm, outStrm) ->
    @dataSameKeyCb = dataSameKeyCb
    @dataNewKeyCb = dataNewKeyCb
    @endCb = endCb
    @inStrm = inStrm
    @outStrm = outStrm
    super()
  
  reduce : ->
    line = ''
    lastKey = ''
    key = ''
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    lazy = new Lazy(@inStrm)
    lazy.lines.map(String).map (line) =>      
      # Returns an array of the key part of the line and the rest of the line
      splitKeyAndRest = (line) ->        
        ret = line.trim().split("\t")
        ret = ['', ''] if typeof ret is 'undefined' or ret.length == 0
        # If split only brought back stuff to left of delim, make that key and append empty rest
        ret.push '' if ret.length == 1
        ret
      #
      [key, rest] = splitKeyAndRest line
      key = key.trim()
      rest = rest.trim()
      
      if key.length > 0
        if key is lastKey 
          @emit 'dataSameKey', key, rest, @outStrm, @dataSameKeyCb, @buildKeyAndRest
        else
          # TODO Horrible to check this every time through the loop for not being initial condition
          if lastKey.length > 0
            @emit 'dataNewKey', lastKey, rest, @outStrm, @dataNewKeyCb, @buildKeyAndRest
          lastKey = key
    
    @emit 'end', @inStrm, @outStrm, @endCb
    return
  
  # Builds a string with the key and the rest of the line delimited by tab
  buildKeyAndRest: (key, rest) -> key + "\t" + rest + "\n"


class StructuredReducerBase extends EventEmitter
  constructor: (dataSameKeyCb, dataNewKeyKb, endCb, inStrm, outStrm) ->
    @dataSameKeyCb = dataSameKeyCb
    @dataNewKeyKb = dataNewKeyKb
    @endCb = endCb
    @inStrm = inStrm
    @outStrm = outStrm
    super()

  reduce: (delim, keyIndexes) ->
    line = ''
    lastKey = ''
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    new Lazy(@inStrm).lines.map(String).map (line) =>
      line = line.trim()
      rec = []      
      rec.push((tkn = tkn.trim())) for tkn in line.split(delim) when tkn.length > 0
      
      # Returns an array of arrays, first are values from key fields, second are values from rest of fields
      splitKeyAndRest = (rec, keyIndexes) ->
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
        #
        [key, rest]
          
      [key, rest] = splitKeyAndRest(rec, keyIndexes)
      key = key.trim()
      rest = rest.trim()      
      if key.length > 0
        if key is lastKey
          # For multiple rows on same key, client maintains state in callback
          @emit 'dataSameKey', key, rest, @outStrm, @dataSameKeyCb, @buildKeyAndRest
        else
          # For first row of new key, client can reset state for first row of new key
          #  and build an output for to emit lastKey and state from dataSameKey handler calls
          @emit 'dataNewKey', lastKey, rest, @outStrm, @dataNewKeyCb, @buildKeyAndRest
          lastKey = key
      #
      return
    #
    @emit 'end', @inStrm, @outStrm, @endCb
    return

  # Now stringify because building output to write to outStrm
  buildKeyAndRest: (key, rest, delim) -> 
    key.join(delim) + "\t" + rest.join(delim) + "\n"
    

# Separate parent class to have mapper object and handle data and end events on it
# Also set defauls here. May come in handy to extend the design here.
class Reducer
  constructor: (dataSameKeyCb = null, dataNewKeyKb = null, endCb = null, inStrm = null, outStrm = null) ->
    # Default values will create pass through mapper from stdin to stdout
    dataSameKeyCb or= (data, outStrm) -> outStrm.write data
    dataNewKeyKb or= (data, outStrm) -> outStrm.write data    
    endCb or= (inStrm, outStrm) -> inStrm.resume()
    inStrm or= process.stdin
    outStrm or= process.stdout
    @reducer = new ReducerBase(dataSameKeyCb, dataNewKeyKb, endCb, inStrm, outStrm)

  reduce: ->
    @reducer.on 'dataSameKey', (key, rest, outStrm, dataSameKeyCb, buildLineCb) -> 
      dataSameKeyCb(key, rest, outStrm, buildLineCb)    
    @reducer.on 'dataNewKey', (key, rest, outStrm, dataNewKeyCb, buildLineCb) -> 
      dataNewKeyCb(key, rest, outStrm, buildLineCb)
    @reducer.on 'end', (inStrm, outStrm, endCb) -> endCb(inStrm, outStrm)
    @reducer.reduce()


class StructuredReducer
  constructor: (dataSameKeyCb = null, dataNewKeyKb = null, endCb = null, inStrm = null, outStrm = null) ->
    # Default values will create pass through mapper from stdin to stdout
    dataSameKeyCb or= (data, outStrm) -> outStrm.write data
    dataNewKeyKb or= (data, outStrm) -> outStrm.write data    
    endCb or= (inStrm, outStrm) -> inStrm.resume()
    inStrm or= process.stdin
    outStrm or= process.stdout
    @reducer = new StructuredReducerBase(dataSameKeyCb, dataNewKeyKb, endCb, inStrm, outStrm)

  reduce: (delim) ->
    @reducer.on 'dataSameKey', (data, outStrm, dataSameKeyCb) -> 
      dataSameKeyCb(data, outStrm)
    @reducer.on 'dataNewKey', (data, outStrm, dataNewKeyCb, buildLineCb) -> 
      dataNewKeyCb(data, outStrm, buildLineCb)
    @reducer.on 'end', (inStrm, outStrm, endCb) -> endCb(inStrm, outStrm)
    @reducer.reduce(delim)

# Export the class to allow construction with or without outStrm arg
exports.Reducer = Reducer
exports.StructuredReducer = StructuredReducer

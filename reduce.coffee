`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

Lazy = require 'lazy'
U = require './util.js'
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
  constructor: (dataSameKeyCb, dataNewKeyKb, endCb, inStrm, outStrm) ->
    @dataSameKeyCb = dataSameKeyCb
    @dataNewKeyKb = dataNewKeyKb
    @endCb = endCb
    @inStrm = inStrm
    @outStrm = outStrm
    super()
  
  reduce : ->
    line = ''
    lastKey = ''
    # Invoke function in 'this' context of Mapper#map() so #emit() binds correctly
    new Lazy(@inStrm).lines.map(String).map (line) =>      
      # Returns an array of the key part of the line and the rest of the line
      splitKeyAndRest: (line) ->
        ret = line.trim().split("\t")
        ret = ['', ''] if typeof obj is 'undefined' or ret.length == 0
        # If split only brought back stuff to left of delim, make that key and append empty rest
        ret.push '' if ret.length == 1
        ret
      
      [key, rest] = splitKeyAndRest line
      if lastKey isnt key
        @emit 'dataSameKey', key, rest, @outStrm, @dataSameKeyCb, @buildKeyAndRest
      else
        @emit 'dataNewKey', key, rest, @outStrm, @dataNewKeyCb, @buildKeyAndRest
      
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
          
      [key, rest] = splitKeyAndRest(rec, keyIndexes)
      if lastKey is key
        # For multiple rows on same key, client maintains state in callback
        @emit 'dataSameKey', key, rest, @outStrm, @dataSameKeyCb
      else
        # For first row of new key, client can reset state for first row of new key
        #  and build an output for to emit lastKey and state from dataSameKey handler calls
        @emit 'dataNewKey', lastKey, key, rest, @outStrm, @dataNewKeyCb, @buildKeyAndRest
        lastKey = key
    
    @emit 'end', @inStrm, @outStrm, @endCb
    return

  # Now stringify because building output to write to outStrm
  buildKeyAndRest: (key, rest, delim) -> 
    kOut = []
    rOut = []
    kOut.push k for k in key
    rOut.push r for r in rest
    kOut.join(delim) + "\t" + rOut.join(delim) + "\n"
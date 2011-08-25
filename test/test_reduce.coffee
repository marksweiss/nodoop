`#!/usr/local/bin/node
//`

HadoopReducer = require('../map.js').HadoopReducer

# TODO
dataCb = (data, outStrm, splitLineCb, buildLineCb) ->
  return

reducer = new HadoopReducer(dataCb)
inStrm = process.stdin

reduce = (inStrm) ->  
  reducer.reduce()
  return

reduce(process.stdin)
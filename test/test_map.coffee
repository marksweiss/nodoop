`#!/usr/local/bin/node
//`

HadoopMapper = require('../map.js').HadoopMapper

dataCb = (data, outStrm, splitLineCb, buildLineCb) ->
  areaCodeToCity = (phoneNum) ->
    return 'Manhattan' if phoneNum[0..2] == '212'
    return 'Unknown'

  [key, rest] = splitLineCb data
  rest = rest.split('|')
  phoneNum = rest[1]
  city = areaCodeToCity phoneNum
  rest.push city
  rest = rest.join('|')
  line = buildLineCb(key, rest)
  outStrm.write(line)
  return

mapper = new HadoopMapper(dataCb)
inStrm = process.stdin

map = (inStrm) ->  
  mapper.map()
  return

map(process.stdin)
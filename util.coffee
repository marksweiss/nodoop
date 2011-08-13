print = (msg) ->
  console.log msg
exports.print = print

isArray = (value) ->
	Object.prototype.toString.apply(value) == '[object Array]'
exports.isArray = isArray

compareArray = (a1, a2) ->
  if (not isArray(a1)) or (not isArray(a2))
    return false
  len1 = 0
  false if (len1 = a1.length) != (a2.length)
  for j in [0..len1]    
    if a1[j] != a2[j]
      return false
  true
exports.compareArray = compareArray
  
printAssert = (expected, actual, result, passMsg, failMsg) ->
  if result
    print passMsg
  else
    print "EXPECTED #{expected} != ACTUAL #{actual}\n"
    print failMsg
exports.printAssert = printAssert
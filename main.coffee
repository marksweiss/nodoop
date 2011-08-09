main = (args) ->
  procArgs = args.slice(2)

  # TEMP DEBUG
  console.log procArgs
  console.log arg for arg in procArgs

  inFilePath = procArgs[0]
  outFilePath = procArgs[1]
  # TODO arbitrary list of mappers and reducers
  mapper = procArgs[2] 
  reducer = procArgs[3]
  
  [inFilePath, outFilePath, mapper, reducer]

# TODO Call here with main(process.argv) IF run from CLI
exports.main = main

# TODO PASS THIS TEST
# TODO REAL TEST FRAMEWORK
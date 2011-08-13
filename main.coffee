conf = require('./config.js').config

getCli = (args) ->
  inFilePath = args[0]
  outFilePath = args[1]
  mapper = args[2] 
  reducer = args[3]
  
  hadoopCli = "#{conf.hadoopBinPath}/hadoop jar #{conf.hadoopHome}/contrib/streaming/hadoop-*-streaming.jar \\\n
-input #{inFilePath} \\\n
-output #{outFilePath} \\\n
-mapper #{mapper} \\\n
-reducer #{reducer}"

  hadoopCli

exports.getCli = getCli
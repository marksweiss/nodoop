conf = require('./config.js').config

getCl = (args) ->
  inFilePath = args[0]
  outFilePath = args[1]
  mapper = args[2] 
  reducer = args[3]
  
  hadoopCl = "#{conf.hadoopBinPath}/hadoop jar #{conf.hadoopStreamingJar} \\\n
-input #{inFilePath} \\\n
-output #{outFilePath} \\\n
-mapper #{mapper} \\\n
-reducer #{reducer} \\\n
-file #{mapper} \\\n
-file #{reducer}"
  hadoopCl

exports.getCl = getCl

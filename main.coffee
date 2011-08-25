conf = require('./config.js').config
U = require './util/util.js'
exec = require('child_process').exec

# TODO Real options handling. Options by bare index position in both of these functions

getJobCl = (args) ->
  inFilePath = args[0]
  outFilePath = args[1]
  mapper = args[2] 
  reducer = args[3]
  
  hadoopCl = "#{conf.hadoopBinPath}hadoop jar #{conf.hadoopStreamingJar} \\\n
-input #{inFilePath} \\\n
-output #{outFilePath} \\\n
-mapper #{mapper} \\\n
-reducer #{reducer} \\\n
-file #{mapper} \\\n
-file #{reducer}"
  hadoopCl

run = (args) ->
  inFilePath = args[0]
  outFilePath = args[1]
  mapper = args[2] 
  reducer = args[3]

  # Delete the output directory if it already exists, this will fail Hadoop streaming job
  removeOutputDirCl = "#{conf.hadoopBinPath}hadoop dfs -rmr #{outFilePath}"
  exec removeOutputDirCl, (err, stdout, stderr) ->
    U.printErr err if err?
    return if err? and err.code != '255'  # '255 == No such file or directory'
    # Clear previous files from HDFS path for input files if flag set
    clearInFilesFlag = args[4]
    clearInFilesCl = "#{conf.hadoopBinPath}hadoop dfs -rm #{inFilePath}*.txt"
    if clearInFilesFlag
      exec clearInFilesCl, (err, stdout, stderr) ->
        U.printErr err if err?
        return if err? and err.code != '255' # '255 == No such file or directory'
        # Load the input files for this run from local directory into HDFS
        loadInFilesCl = "#{conf.hadoopBinPath}hadoop dfs -put #{inFilePath}*.txt #{inFilePath}"        
        child = exec loadInFilesCl, (err, stdout, stderr) ->
          if err?
            U.printErr err
          else
            # Run the job
            jobCl = getJobCl(args)
            child = exec jobCl, (err, stdout, stderr) ->
              if err? then U.printErr err else U.print stdout
    else
      # Load the input files for this run from local directory into HDFS
      loadInFilesCl = "#{conf.hadoopBinPath}hadoop dfs -put #{inFilePath}*.txt #{inFilePath}" 
      child = exec loadInFilesCl, (err, stdout, stderr) ->
        if err?
          U.printErr err
        else
          # Run the job
          jobCl = getJobCl(args)
          child = exec jobCl, (err, stdout, stderr) ->
            if err? then U.printErr err else U.print stdout


# NOTE: should only call this to test and debug
exports.getJobCl = getJobCl
# NOTE: only need to call this to run jobs
exports.run = run
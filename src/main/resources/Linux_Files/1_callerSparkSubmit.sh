#!/bin/bash
#
# This is the single script executed on the linux cluster edge node.
#sh authentication.sh

# WARNING do not specify the locations of files by ~/blaBlaBla   use the ./  .//./ formulation instead
# The reason is that the propertiesFileHandler.scala fails to see ~/ formulation.  I know... kinda lame.
configFid="/home/developer/tpmoyer/oneoffs/z_template_hdfs.config"
. $configFid
propertiesFid="/home/developer/tpmoyer/oneoffs/z_template_hdfs.properties"
. $propertiesFid

# There are, at a minimum, three inputs you need to give ()
# 1) propertiesFid, which can contain inputs numbered 2 and 3
# 2) class=   The name of the java/scala object class to be called  
# 3) jarFid=  The name of the .jar which contains the class

appStart=$(date  +%s%N | cut -b1-13) 
me=`basename "$0"`
meBase=${me:0:${#me}-3}
echo "meBase=$meBase"  |& tee -a $logFid
appStartDateTime=$(date +'%Y%m%d_%H_%M_%S')
logFid=$1
if [ ${#logFid} == 0 ]; then
	echo "zero Length arg[1] prompted autogeneration of logFid"
	mkdir -p ./logs
	logFid=./logs/${meBase}_$appStartDateTime.log
fi
echo ""  |& tee -a $logFid
echo "$me"                                         |& tee -a $logFid
echo "appStartDateTime=$appStartDateTime"          |& tee -a $logFid
echo "appStart=$appStart milliseconds sence epoch" |& tee -a $logFid
echo "beelineOptions=$beelineOptions"              |& tee -a $logFid
echo "impalaOptions=$impalaOptions"                |& tee -a $logFid
echo "request_pool=$request_pool"                  |& tee -a $logFid
echo "logFid=$logFid"                              |& tee -a $logFid
echo "jarFid=$jarFid"                                           |& tee -a $logFid
echo "class=$class"                                             |& tee -a $logFid
echo "tables2ImportSpreadsheetFid=$tables2ImportSpreadsheetFid" |& tee -a $logFid
echo ""  |& tee -a $logFid
retCode=0
failRetCode=1
shopt -s nocasematch   # set compare to be case insensitive

echo "${meBase} location marker 1 $(date +'%Y%m%d_%H_%M_%S') retCode=$retCode"  |& tee -a $logFid

#sparkExecutorMemory=5g
#sparkDriverMemory=5g
#sparkMinExecutors=6
#sparkMaxExecutors=12
#sparkDefaultParallelism=372
#sparkSqlShufflePartitions=372

djo=$'-Dspark.dynamicAllocation.minExecutors='${sparkMinExecutors}$' '
djo+=$'-Dspark.dynamicAllocation.maxExecutors='${sparkMaxExecutors}$' '
djo+=$'-Dspark.default.parallelism='${sparkDefaultParallelism}$' '
djo+=$'-Dspark.sql.shuffle.partitions='${sparkSqlShufflePartitions}$' '
#djo+=$'-Dlog4j.configurationFile=log4j2.properties'


executionLocation="onEdgeNode"
#executionLocation="insideCluster"
if [ $executionLocation = "onEdgeNode" ]; then
	target=$'\n--master yarn \n'
	target+=$'--deploy-mode client \n'
else
	 target=$'\n--master yarn \n'
	target+=$'--deploy-mode cluster \n'
fi	

# any changes would be to the ${calledVariables} 
#target+=$'--queue '${request_pool}$' \n'
target+=$'--name CoalesceParquets_V1.0 \n'
target+=$'--driver-memory '${sparkDriverMemory}$' \n'
target+=$'--executor-memory '${sparkExecutorMemory}$' \n'
#target+=$'--files /etc/hive/conf/hive-site.xml \n'

#target+=$'--jars ./Knowledgent_Ingestion_Framework-V2.02_lib/config-1.2.1.jar \n'
#target+=$'--driver-class-path "/opt/cloudera/parcels/CDH/lib/hive/lib/*" \n'

# these will be specific to each script.
# WARNING do not specify the locations of files by ~/blaBlaBla  The propertiesFileHandler fails to see these
target+=$'--class '${class}$' \n'
target+=${jarFid}$' \n'
target+=${logFid}$' \n' 
target+=${configFid}$' \n'
target+=$'./log4j_linux.properties  \n'
target+=${propertiesFid}$' \n'
target+=${tables2ImportSpreadsheetFid}

# if you want to do the spark-csv you will need to access 
# the spark-csv and the commons-csv jars 
#target+=$'--jars ./spark-csv_2.10-1.3.0.jar,./commons-csv-1.2.jar,./ScalaSparkSql-1.01.jar \n'


echo "${meBase} location marker 1 $(date +'%Y%m%d_%H_%M_%S') retCode=$retCode"  |& tee -a $logFid
# the standard, say what you're going to do, and do it
echo "spark-submit"                                 |& tee -a $logFid
echo "--driver-java-options $djo"                   |& tee -a $logFid
echo "$target"                                      |& tee -a $logFid
spark-submit --driver-java-options "$djo" ${target} |& tee -a $logFid
retCode=$?
echo "${meBase} location marker 2 $(date +'%Y%m%d_%H_%M_%S') retCode=$retCode"  |& tee -a $logFid


#sh 3_sniffTableRowCounts.sh |& tee -a $logFid
#retCode=$?
#echo "${meBase} location marker 2 retCode=$retCode"  |& tee -a $logFid

state="FAILED"
if [ $retCode -eq 0 ]; then
	state="SUCCESSFULL"
fi
appEnd=$(date  +%s%N | cut -b1-13)
DIFF=$(echo "$appEnd - $appStart" | bc)
me=`basename "$0"`
printf "$state $me completed in %.3f seconds. retCode=$retCode\n" $(bc -l <<< "( $DIFF  / 1000)")  |& tee -a $logFid
echo "" |& tee -a $logFid
exit $retCode


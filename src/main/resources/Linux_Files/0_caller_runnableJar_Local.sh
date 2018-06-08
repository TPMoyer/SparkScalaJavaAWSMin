#!/bin/bash 

awsConfigFid=/home/developer/tpmoyer/framework/passwords/takeda.config
configFid=./z_template_hdfs.config
propertiesFid=./z_template_hdfs.properties 
. $awsConfigFid
. $configFid
. $propertiesFid

appStart=$(date  +%s%N | cut -b1-13)  
me=`basename "$0"` 
meBase=${me:0:${#me}-3} 
echo "meBase=$meBase" 
appStartDateTime=$(date +'%Y%m%d_%H_%M_%S') 
logFid=$1 
if [ ${#logFid} == 0 ]; then 
	echo "zero Length arg[1] prompted autogeneration of logFid" 
	mkdir -p ./logs 
	logFid=./logs/${meBase}_$appStartDateTime.log 
	#logFid=./logs/${meBase}.log 
fi 
echo "" 
echo "$me"                                         |& tee -a $logFid 
echo "appStartDateTime=$appStartDateTime"          |& tee -a $logFid 
echo "appStart=$appStart milliseconds sence epoch" |& tee -a $logFid 
echo "logFid=$logFid"                              |& tee -a $logFid 
echo "" |& tee -a $logFid 
retCode=0 
failRetCode=1 
shopt -s nocasematch   # set compare to be case insensitive 

# logging is messed up when the log4j is inside the .properties file
#java -jar ./Knowledgent_Ingestion_Framework-V2.02.jar $propertiesFid

# works, but no configurations passed in as CLI

# say what you're going to do, then to it
echo "java  \\"                     |& tee -a $logFid 
echo "-jar $jarFid \\"              |& tee -a $logFid 
echo "-Xms$initialJVMMemoryPool \\" |& tee -a $logFid 
echo "./log4j_linux.properties \\"  |& tee -a $logFid 
echo "$awsConfigFid \\"             |& tee -a $logFid 
echo "$configFid \\"                |& tee -a $logFid 
echo "$propertiesFid "              |& tee -a $logFid 


java \
-jar $jarFid \
-Xms$initialJVMMemoryPool \
./log4j_linux.properties \
$awsConfigFid \
$configFid \
$propertiesFid \
|& tee -a $logFid 


# the purest run-naked runnable-jar
#echo "java -jar $jarFid "   |& tee -a $logFid 
#      java -jar $jarFid     |& tee -a $logFid 



state="FAILED" 
if [ $retCode -eq 0 ]; then 
	state="SUCCESSFULL" 
fi 
appEnd=$(date  +%s%N | cut -b1-13) 
DIFF=$(echo "$appEnd - $appStart" | bc) 
me=`basename "$0"` 
printf "$state retCode=$retCode  $me completed in %.3f seconds\n" $(bc -l <<< "( $DIFF  / 1000)")  |& tee -a $logFid 
echo "" |& tee -a $logFid 
exit $retCode 

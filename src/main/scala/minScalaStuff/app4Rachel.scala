package minScalaStuff
/* expand this to show a workable default .properties file entry
 *  
./Windows_Files/z_template_windows.properties

 */
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, Logger, LogManager, PropertyConfigurator}
import com.typesafe.config.{ConfigFactory,Config,ConfigList,ConfigException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,LocalFileSystem,Path,PathFilter,FileStatus,LocatedFileStatus,RawLocalFileSystem}
import java.io.{File, FileReader,BufferedOutputStream, FileOutputStream}
import org.apache.hadoop.fs.FileSystem._

import scala.collection.JavaConversions._
import java.util.Properties
import scala.util.parsing.json._
import scala.tools.nsc._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.immutable.ListMap
import sys.process._
import java.net.URL
import java.util.Date;

import java.lang.Math._
import myScalaUtils.FileIO._
import myScalaUtils.PropertiesFileHandler._
import myScalaUtils.MiscellaneousUtils._

import awscala._, s3._
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.regions.Regions._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client._

// /*import com.amazonaws.services.s3.model._ /* NOOoooo this conflict with the s3._, so add methods and types incrementally */
import com.amazonaws.services.s3.model.{
  AbortMultipartUploadRequest,
  CompleteMultipartUploadRequest,
  CopyObjectRequest,
  CopyPartRequest,
  CopyPartResult,
  InitiateMultipartUploadRequest,
  PartETag,
  PutObjectRequest,
  UploadPartRequest
}
import com.amazonaws.services.s3.transfer.{TransferManager,ObjectMetadataProvider}


import java.text.SimpleDateFormat

object app4Rachel {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))

  def yourStuffGoesHere(
    args : Array[String]
    ,config : Config           /* typesafe configuration */
    ,spark : SparkSession
    ,fs    : FileSystem      /* if you run this on your laptop, is your wind or mac filesystem.  When run through spark-submit on a cluster, is HDFS */ 
    ,lfs   : LocalFileSystem /* When run on the cluster, lfs LocalFileSystem is the NFS of the master node */
    ,conf  : Configuration   /* the spark configuration */
  ): Int = {
    log.info("cme@ myApp with args.length="+args.length)
    var ranWithErrors=0
    var msg=""
    var finalMessage=""
    /****************************************************************************************************************/
    /**********************  replace the stuff below, and between these headers with your code   ********************/
    /****************************************************************************************************************/
    var tableName="ipt_query"
    var sql_fid="/dev/omop/refined/rachel/sql/"+tableName+".sql"
    log.info("about to attempt read on "+sql_fid)
    val sqlFile = spark.sparkContext.textFile(sql_fid)
    
    var par_fid="/dev/omop/refined/rachel/"+tableName+"_tom_try0/"
    val sqlDF = spark.sql(sqlFile.collect().mkString("\n"))
    print("count: " + sqlDF.count()+"\n")
    sqlDF.show(15)
    sqlDF.write.parquet(par_fid)
    
    log.info("about to attempt write on "+par_fid)
    var df=spark.read.parquet(par_fid)
    df.createOrReplaceTempView(tableName)
    
    val queryDf=spark.sql("select * from "+tableName+" limit 40")
    
    log.info(showString(queryDf,80))


    /****************************************************************************************************************/
    /****************    boilerPlate below here   *******************************************************************/
    /****************************************************************************************************************/
    
    
    log.info("at endof yourStuffGoesHere ranWithErrors="+ranWithErrors)
    log.error(finalMessage)
    println(finalMessage)
    ranWithErrors
  }
  def run(args : Array[String]):Int = {
    val appName=this.getClass.getName.substring(0,this.getClass.getName.length-1)
    println("cme@ appName="+appName +" run(args : Array[String]) with "+args.length+" args")
    for((arg,ii)<-args.zipWithIndex)println(f"arg($ii%2d) $arg%s")
    var msg="this method will build a sparkSesson, initialize your hadoop.fs api, and connect to the hive metaDataStore.\nShould take about 7 seconds.";println(msg);log.fatal(msg);
    
    var retVal = 0
    var exitCodeFid="./logs/exitCode.txt"
    if(System.getProperty("os.name").startsWith("Windows"))exitCodeFid="c:\\logs\\ExitCode.txt"
    val sb = new StringBuilder()
    var appBeginTime=System.nanoTime
    sb.append(appName+" started at "+appBeginTime)
    
    val conf=new Configuration() /* needed for both hdfs and local FileSystem instancing */
    val fs = FileSystem.get(conf)
    val rlfs = new RawLocalFileSystem() /* If we are not on a cluster, fs and lfs will be equal */
    rlfs.setConf(conf);
    val lfs=new LocalFileSystem(rlfs);
    try {
      var gotAnArgFidProperties=false
      val target0=".properties"
      val target1="log4j"
      var argFidT2IMD=""    
      var config = ConfigFactory.empty()
      var reArgs = new Array[String](1)
      var argInputFid=""
      var argOutputLocation=""
      val numPropertiesFiles=args.map { x => if((x.length>=target0.length) && ( 0==x.substring(x.length()-target0.length()).compareToIgnoreCase(target0))) 1 else 0 }.foldLeft(0)(_+_)
      sb.append("saw "+numPropertiesFiles+" properties files\n")
      if(0==numPropertiesFiles){
        val singlePropFid="z_KnowledgentIngestionFramework_singleFile.properties"
        val singleProp = new File("./"+singlePropFid)
        if(singleProp.exists()){
          //println("required singleFile.properties file ("+singlePropFid+") is present in current directory")
          reArgs = new Array[String](1+args.length)
          reArgs(0)=singlePropFid
          for((a,ii)<-args.zipWithIndex)reArgs(ii+1)=args(ii)
        } else {
          println("\ncurrent directory shows as\n"+new File("./").getCanonicalPath)
          println("\nIf a .properties file is not in the CLI list,\n"+singlePropFid+"\nmust be present in the current directory.\n\nICDC_Framework FAILED because no .properties file was found")
          fileO(fs,lfs,true,exitCodeFid, new StringBuilder().append(retVal))
          System.exit(retVal)
        }
      } else {
        reArgs=args
      }
      reArgs.foreach { arg =>
        log.info("arg="+arg)
        var handled=false
        if((   (arg.length>=target0.length) && (0==(arg.substring(arg.length()-target0.length())).compareToIgnoreCase(target0)) )) {
          //this.logDebug("see properties "+arg)
          config=propertiesFileHandler(arg,config,numPropertiesFiles)
          log.info("came out of propertiesFileHandler ok")
          gotAnArgFidProperties=true
          handled=true
        } 
        if(!handled){
          sb.append("argument not handled:"+arg+"\n")
        }
      }
      log.info(sb)
      log.info("preLaunch")
      if(gotAnArgFidProperties){
        Logger.getLogger("io" ).setLevel(Level.WARN)  /* preempt 30ish rows of DEBUG. to-do: Do I use Netty? prompted by comment on Javassist can speedup Netty */
        Logger.getLogger("org").setLevel(Level.ERROR) /* set this to Level.INFO to get the spark default quantity of console output */
        //Logger.getLogger("org").setLevel(Level.INFO)  /* run the app once against a small file,  with this uncommented, then look for the    SparkUI   */ 
        Logger.getLogger("log").setLevel(Level.INFO)
        log.info("set loggers ok")
        
        log.info("SparkSession.builder using "+(if(config.getBoolean("onCluster"))" externally specified master" else " specifiying master(\"local[*]\")"))
        val spark = if(config.getBoolean("onCluster"))
          SparkSession.builder().enableHiveSupport().getOrCreate() 
        else 
          SparkSession.builder().master("local[*]").getOrCreate()
          
        import spark.implicits._   /* this is used to implicitly convert an RDD to a DataFrame. */ 
        logFSStuff(fs,rlfs,lfs,conf)
        showHiveDatabases(spark)    
        /**************************************************  Launch the payload ****************************/
        config=tackonSysEnv(config)
        //msg="Launching yourStuffGoesHere pre-empted by this \"safty on\"\nComment this row to enable \"yourStuffGoesHere\" method.";println(msg);log.fatal(msg);System.exit(1);
        msg="SparkSession built, and connection to hiveMetaDataStore established. Launching yourStuffGoesHere"
        retVal+=yourStuffGoesHere(args,config,spark,fs,lfs,conf)
        /****************************************************************************************************/
      } else {
        println("\nrequired inputs not present.\nThere must be at least one .properties file")
        retVal=1
      }
    } catch {
      case e: Exception =>
        retVal=2
        log.error("an exception occurred under  "+appName+":\n" + ExceptionUtils.getStackTrace(e))
        println("\n\n************************************************* 0 Failed with exception ********************************************************\n")
        throw new Exception("Error")
      case unknown : Throwable =>
        retVal=4
        log.error("an exception occurred under  "+appName+":\n" + ExceptionUtils.getStackTrace(unknown))
        println("\n\n************************************************* 1 Failed with unknown type exception *******************************************\n")
        throw new Exception("unknown Error")      
    } finally {
      /* output retval (standard 0==good anythingEelse==fail) to an exitCode file to allow acquisition of a real exit code, even if this were called by another jar like yarn or spark-submit */
      log.debug("retval="+retVal)
      fileO(fs,lfs,true,exitCodeFid, new StringBuilder().append(retVal))
      val msg="\n"+(if(0==retVal)"SUCCESSFUL"else"FAILED")+f" framework run took  ${(System.nanoTime-appBeginTime)/1e9}%8.3f seconds"
      println(msg)
      log.info(msg)
    }
    return retVal
  }

  def main(args : Array[String]){
    val retVal = run(args)
    System.exit(retVal)
  }

}

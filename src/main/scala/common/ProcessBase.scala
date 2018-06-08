package common

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.{ConfigFactory, Config}
import java.util.Properties
import utils._
import java.time._
import java.time.format.DateTimeFormatter
import utils.SparkIOImplicits._
import utils.MiscellaneousUtils._

trait ProcessBase {
	@transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))
	
	/* prototypes to be overridden by object which extends this trait*/
	def run(config:Config                     ): Integer
	def run(config:Config, spark: SparkSession): Integer 
	
	def setupConfig(args: Array[String],appName : String,appBeginTime:Long):Config = {
		val sb = new StringBuilder()
		var msg="cme@ ProcessBase.setupConfig";sb.append(msg+"\n")
		var gotAnArgFidProperties=false
		val target0=".config"
		val target1=".properties"
		var argFidT2IMD=""
		var config = ConfigFactory.empty()
		
		val targets = Seq(".config", ".properties")
		val numPropertiesFiles=targets.map{t=> args.map { x => if((x.length>t.length)&&(0==x.substring(x.length()-t.length()).compareToIgnoreCase(t)))1 else 0 }.foldLeft(0)(_+_)}.foldLeft(0)(_+_)
		msg=appName+" started. "+LocalDateTime.now.toString.replace("T"," ")+"  saw "+numPropertiesFiles+" properties files"
		println(msg)
		sb.append(msg+"\n")
		for ((arg,ii)<-args.zipWithIndex){msg=f"arg($ii%2d)=$arg%s";println(msg);sb.append(msg+"\n")}
			
		//println(System.getProperty("os.name")+" "+System.getProperty("user.dir"))
		if(0==numPropertiesFiles) {
			var fid=System.getProperty("user.dir")+"/src/main/resources/"+
				(if(System.getProperty("os.name").startsWith("Window"))"Windows" else "Linux")+
				"_Files/log4j_"+(if(System.getProperty("os.name").startsWith("Window"))"windows" else "linux")+".properties"
			if(new java.io.File(fid).exists){
				val fidSay=if(System.getProperty("os.name").startsWith("Window"))fid.replace("/","\\") else fid.replace("\\","/")
				//val fidSay=fid.replace("\\",	(if(System.getProperty("os.name").startsWith("Window"))"\\" else "/"))
				msg="no .config or .properties recieved as CLI args.  Eclipse log4j.properties detected and is being applied\nfid="+fidSay
				println(msg)
				sb.append(msg)
				config=PropertiesHandler.propertiesHandler(fid,config,numPropertiesFiles)
			} else {
				msg="no .config or .properties recieved as CLI args, and the default eclipse log4j.properties could not be found.\nImplementing default logging to current location.\n"+System.getProperty("user.dir")+"/"+appName+".log"
				println(msg)
				sb.append(msg)
				val sbF=new StringBuilder()
				sbF.append("log4j.rootLogger=INFO,fileout,fileout0\n")
				sbF.append("# notes:\n")
				sbF.append("#  Threshold takes precedence over programatic log.setLevel \n")
				sbF.append("#  The $ variables are set within PropertiesFileHandler.scala prior to calling the PropertyConfigurator.configure\n")
				sbF.append("\n")
				sbF.append("# Direct log messages to file name which does not change run to run\n")
				sbF.append("log4j.appender.fileout0=org.apache.log4j.FileAppender\n")
				sbF.append("log4j.appender.fileout0.File=./"+appName+".log\n")
				sbF.append("log4j.appender.fileout0.ImmediateFlush=true\n")
				sbF.append("log4j.appender.fileout0.Threshold=debug\n")
				sbF.append("log4j.appender.fileout0.Append=false\n")
				sbF.append("log4j.appender.fileout0.layout=org.apache.log4j.PatternLayout\n")
				sbF.append("log4j.appender.fileout0.layout.conversionPattern=%-5p %8r %3L %c{1} - %m%n\n")
				import java.io._
				fid="./log4j.properties"
				val bw = new BufferedWriter(new FileWriter(new File(fid)))
				bw.write(sbF.toString())
				bw.close
				config=PropertiesHandler.propertiesHandler(fid,config,numPropertiesFiles)				
			}
			//val log4jFid=(if(System.getProperty("os.name").startsWith("Window")
		}else {
			println("")
		}
		
		args.foreach { arg =>
			var handled=false
			if(  (  (arg.length>target0.length)
					   &&(0==arg.substring(arg.length()-target0.length()).compareToIgnoreCase(target0))
					  )
					||(  (arg.length>target1.length)
					   &&(0==arg.substring(arg.length()-target1.length()).compareToIgnoreCase(target1))
					  )
				){
				//this.logDebug("see properties "+arg)
				config=PropertiesHandler.propertiesHandler(arg,config,numPropertiesFiles)
				gotAnArgFidProperties=true
				handled=true
			}
			if(  arg.toLowerCase().endsWith(".log")){
				val props= new Properties()
				props.setProperty("logFid",arg)
				config=ConfigFactory.parseProperties(props).withFallback(config)
				handled=true
			}
			if(!handled){
				printAndLog("argument not handled:"+arg+"\n")
			}
		}
		
		config=PropertiesHandler.tackonSysEnv(config)
		log.info(sb)
		return config;  
	}
	
	def go4it(
		 config: Config
		,printFileSystemStuff: Boolean
		,includeSpark : Boolean
		,checkHive: Boolean,printHiveDatabases:Boolean=true
	): Int = {
		log.info("cme@ go4it")
		
		val sb = new StringBuilder()
		var ranWithErrors = 0
		
		Logger.getLogger("io").setLevel(Level.WARN) /* preempt 30ish rows of DEBUG. to-do: Do I use Netty? prompted by comment on Javassist can speedup Netty */
		Logger.getLogger("org").setLevel(Level.ERROR) /* set this to Level.INFO to get the spark default quantity of console output */
		//Logger.getLogger("org").setLevel(Level.INFO)
		Logger.getLogger("log").setLevel(Level.INFO)
		log.info("set loggers ok")
		
		
		val fss=MiscellaneousUtils.getFileSystemStuff()
		log.info(fss)
		if(printFileSystemStuff)println(fss)
		
		if(checkHive){
			val shd=MiscellaneousUtils.showHiveDatabases(SharedProperties.spark)
			log.info("\n"+shd)
			if(printHiveDatabases)println(shd)
		}
		if(includeSpark)run(config, SharedProperties.spark) else run(config)
		
		log.info("atEndOf go4it ranWithErrors=" + ranWithErrors)
		ranWithErrors
	}	

	val setup: Unit = {
		Logger.getLogger("io").setLevel(Level.WARN) /* preempt 30ish rows of DEBUG. to-do: Do I use Netty? prompted by comment on Javassist can speedup Netty */
		Logger.getLogger("org").setLevel(Level.ERROR) /* set this to Level.INFO to get the spark default quantity of console output */
		//Logger.getLogger("org").setLevel(Level.INFO)
		Logger.getLogger("log").setLevel(Level.INFO)
		
		System.setProperty("logNamePart0", "location")
		System.setProperty("appStart.yyyyMMddHHmmss", LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
		import org.apache.log4j.{ Level, Logger, LogManager}
		val log = LogManager.getRootLogger()
		Unit
	}


	def printAndLogTaskMsg(retVal:Long,beginTime:Long,task:String) {
		val msg="\n"+(if(0==retVal)"Success"else"FAIL")+f" retVal="+retVal+ " "+task +f" took ${(System.nanoTime-beginTime)/1e9}%8.3f seconds "
		printAndLog(msg)
	}
}
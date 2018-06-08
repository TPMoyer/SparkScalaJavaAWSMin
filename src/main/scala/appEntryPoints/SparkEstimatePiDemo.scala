package appEntryPoints
/* All the worker-bee objects/classes should go eclipse packages other than appEntryPoints
 * such as the "appSpecificStuff" eclipse package  
 * The appEntryPoints package contains all-and-only-all objects which have main defined.
 * Hopefully, this allows new folks to find the starting points for applications more easily.
*/

/* Expand this comment for suggested program CLI argument and VM arguments
 * 
 * CLI and VM arguments can be supplied to eclipse runs by first establishing a "run configuration"
 * by clicking on the icon bar Run icon pulldown... run as...   scala application
 * 
 * Thereafter icon bar Run icon pulldown... run configurations...   arguments tab
 * shows two sections:   the upper for CLI and the lower for VM
 * In the upper (program arguments:) box is for CLI inputs
.\src\main\resources\Windows\log4j.properties
.\src\main\resources\Windows\z_template_windows.properties

 * the lower (VM:) entryField has arguments for the JVM
 * Note that the suggested parallelism(s) below are much smaller than the spark default 200   
-ea 
-Xmx4g
-Dspark.default.parallelism=2 
-Dspark.sql.shuffle.partitions=2

*/

import org.apache.spark.sql.SparkSession
import common.ProcessBase
import com.typesafe.config.Config
import utils.MiscellaneousUtils._
import common.FileSystemManager._
import utils.FileIO._
import java.io.File
//import org.apache.hadoop.fs.{FileSystem,RawLocalFileSystem,LocalFileSystem,Path,PathFilter,FileStatus,LocatedFileStatus,FSDataInputStream,FSDataOutputStream}
import org.apache.hadoop.fs._

import appSpecificStuff.PiCalculator._

object SparkEstimatePiDemo extends ProcessBase {
	@transient lazy override val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))
	
	/* non-spark version of run is present to allow trait (ProcessBase.scala) to be created. It needs an overload version of run called by go4it with includeSpark==false*/
	override def run(config: Config) : Integer = {0}
	
	/* This run method is where you make the calls which do the actual work of the application. */
	override def run(config: Config, spark: SparkSession) : Integer = {
		var retCode=0
		
		printAndLog("\nconfig.onCluster="+config.getBoolean("onCluster"))
		val pi=	piCalculator(config,spark,1000)
		printAndLog("pi is roughly "+pi)
		
		retCode
	}
	
	/* main remains unchanged from app to app */
	def main(args: Array[String]) {
		val appBeginTime = System.nanoTime
		var retVal = 0   /* used like the standard linux return value 0=good, anything else is a fail */
		val className=this.getClass.getName.substring(0,this.getClass.getName.length-1)
		
		val config=setupConfig(args,className,appBeginTime)
		val printFileSystemStuff=false  /* fileSystemStuff gets logged, this toggles only the console output */
		val checkHive=false  /* if set to true, this will initialize the spark.sqlcontext, which takes some 4 to 6 seconds. */
		val includeSpark=true
		retVal+=go4it(config,printFileSystemStuff,includeSpark,checkHive)
		
		printAndLogTaskMsg(retVal, appBeginTime, className)
		System.exit(retVal)
	}
}
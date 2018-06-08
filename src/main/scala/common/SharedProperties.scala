package common

import org.apache.spark._
import org.apache.spark.sql._
import com.typesafe.config.Config
import java.time._
import java.time.format._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/*
 * This class is intended to be passed via the Dependency Injection Framework to other classes that
 * use the common variables. 
 * This is the place for initialization of each shared variable.
 */

class SharedProperties(_config: Config){
	@transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))
	
	val config: Config = _config
	
	import SharedProperties._
	
	val MissingPropertyErrorCode=87  /* an arbitrary, not likely to be used by others, number */

	private def getProperty[T](f: String =>T,property: String)= ExceptionHandler.callOrExit(MissingPropertyErrorCode, log, Some(errorMessage(property)))(f(property))
	private def errorMessage(property: String) = s"Attempt at setting $property but property not present"
}

object SharedProperties {
		@transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))
			log.warn("sharedProperties    SparkConf instantiations went ok.")
//		val setup : Unit = {
//			System.setProperty("logNamePart0", "location")
//			Unit
//		}
		val appName=if(Option(System.getProperty("spark.app.name")).isDefined) System.getProperty("spark.app.name") else System.getProperty("package.class")
		val spark = if(Option(System.getProperty("SPARK_SUBMIT")).isDefined)
			SparkSession.builder().appName(appName).getOrCreate() 
		else 
			/* This hard codes a modest 2 CPU and 1G RAM per executor, appropriate for small laptops.  
			 * Folks with developer or gaming, or large AWS EC2 instances will want to use larger allocations. 
			 */
			SparkSession.builder().appName(appName).master("local[2]").config("spark.executor.memory","1g").getOrCreate()
		import spark.implicits._   /* this is used to implicitly convert an RDD to a DataFrame. */
		log.warn("SparkConf instantiations went ok.")
		
}		
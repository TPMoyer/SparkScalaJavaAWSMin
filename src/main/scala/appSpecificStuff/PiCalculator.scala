package appSpecificStuff

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import scala.util.Random._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object PiCalculator {
	@transient lazy val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))

	def piCalculator(
		 config : Config
		,spark  : SparkSession
		,numSamples : Integer
	) : Double = {
		/* bit of scala flashyness here: Check the log file to see the pretty-formatted output  */
		/* First line goes thru the stackTrace and gets the length of the longest .getMethodName() */
		/* If the collection you are trying to get the longest length of, is not an Array or Sequence, put .toSeq just before the reduceLeft */
		/* This example is a bit more complex than the normal use, as it wanted the length of a member of the Thread.currentThread.getStackTrace structure */
		/* Most uses would not include the several ".getMethodName"s present in the below stackTrace example of the very-infrequently-used-by-me reduceLeft  */
		/* 
		 * val longestXxxLength=myDataFrameColumnOrOtherCollection.toSeq.reduceLeft((x,y) => if(x.length >= y.length) x else y).length    
		 */
		val longestMethodNameLength=Thread.currentThread.getStackTrace.reduceLeft((x,y) => if(x.getMethodName.length >= y.getMethodName.length) x else y).getMethodName.length
		/* Uses the derived longestXXXLenght in a format so that things will line up vertically, yet be minimal width */
		val f0="cme@ %2d %3d %-"+longestMethodNameLength+"s %s"
		/* push to log, the stack trace of  this method, and all the calling methods.  
		 * The if(0<ii) avoids logging the root of the stack ie Thread.currentThread.getStackTrace(0), because it is a always "getStackTrace"   
		 */
		for((c,ii) <- Thread.currentThread.getStackTrace.zipWithIndex)if(0<ii)log.warn(f0.format(ii,c.getLineNumber,c.getMethodName,c.getFileName))
		
		
		val count = spark.sparkContext.parallelize(1 to numSamples).filter { _ =>
			val x = math.random
			val y = math.random
			x*x + y*y < 1
		}.count()
		
		4.0 * count / numSamples
	}
}
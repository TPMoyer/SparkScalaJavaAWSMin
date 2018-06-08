package utils

import org.apache.spark.sql.{ DataFrameReader, DataFrameWriter, DataFrame }
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scala.util.control.Exception
import common.ExceptionHandler

object SparkIOImplicits {

	val DefaultErrorCode = 1
	
	/*
		* Most Dataframe reads are wrapped to handle errors
		* Returns DataFrames like original methods
		* Add more if necessary
		* 
		* Example usage: 
		* import utils.SparkIOImplicits._
		* then spark.read.unsafeParquet(...)
		*/
	implicit class DataFrameReaderImplicits(dataframeReader: DataFrameReader) {
		@transient lazy val log = org.apache.log4j.LogManager.getLogger(classOf[DataFrameReader])
		
		def csvOrExit    (path: String, errorCode: Int = DefaultErrorCode): DataFrame = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt read on "+path)
			dataframeReader.csv(path)
		}  
		def orcOrExit    (path: String, errorCode: Int = DefaultErrorCode): DataFrame = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt read on "+path)
			dataframeReader.orc(path)
		}
		def parquetOrExit(path: String, errorCode: Int = DefaultErrorCode): DataFrame = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt read on "+path)
			dataframeReader.parquet(path)
		}
		def parquetOrExitMultiSchemaInstances(path: String, errorCode: Int = DefaultErrorCode): DataFrame = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt MultiSchemaInstances read on "+path)
			dataframeReader.parquet(path)
		}
		def textOrExit   (path: String, errorCode: Int = DefaultErrorCode): DataFrame = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt read on "+path)
			dataframeReader.text(path)
		}
		
	}
	
	
	/*
		* Most Dataframe writes are wrapped to handle errors
		* Add more if necessary
		* 
		* Example usage: 
		* import utils.SparkIOImplicits._
		* then dataframe.write.unsafeParquet(...)
		*/
	implicit class DataFrameWriterImplicits[T](dataframeWriter: DataFrameWriter[T]) {
		@transient lazy val log = org.apache.log4j.LogManager.getLogger(classOf[DataFrameWriter[T]])
		
		def csvOrExit    (path: String, errorCode: Int = DefaultErrorCode) = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt write to "+path)
			dataframeWriter.csv(path)
		}
		def orcOrExit    (path: String, errorCode: Int = DefaultErrorCode) = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt write to "+path)
			dataframeWriter.orc(path)
		}
		def parquetOrExit(path: String, errorCode: Int = DefaultErrorCode) = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt write to "+path)
			dataframeWriter.parquet(path)
		}
		def textOrExit   (path: String, errorCode: Int = DefaultErrorCode) = ExceptionHandler.callOrExit(errorCode, log){
			log.info("about to attempt write to "+path)
			dataframeWriter.text(path)
		}
	}
	
	implicit class SparkSessionImplicits(spark: SparkSession) {
		@transient lazy val log = org.apache.log4j.LogManager.getLogger(classOf[SparkSession])
		
		def sqlOrExit(sqlText: String, errorCode: Int = DefaultErrorCode): DataFrame = ExceptionHandler.callOrExit(errorCode, log, Some(s"Failed executing SQL: $sqlText"))(spark.sql(sqlText))
	}
}
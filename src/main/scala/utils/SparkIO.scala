package utils

import SparkIOImplicits._
import org.apache.spark.sql.{SparkSession, DataFrame}
import utils.FileIO._
import utils.MiscellaneousUtils.showString
import common.FileSystemManager
import com.typesafe.config.Config

//import sparkschema.parser.SchemaParser

object SparkIO {
	//@transient lazy val log = org.apache.log4j.LogManager.getLogger("SparkIO")
	@transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))
	//log.info("this.getClass.getName="+this.getClass.getName)
	//log.info("this.getClass.getCanonicalName="+this.getClass.getCanonicalName)
	//log.info("this.getClass..toString="+this.getClass.toString())
		
	private val ViewFromParquetErrorCode = 150
	private val DataFrameFromSQLErrorCode = 151
	
	def createViewFromParquet(spark: SparkSession, path: String, viewName: String): Unit = {
		val df = spark.read.parquetOrExit(path, ViewFromParquetErrorCode)
		log.info("returned from read.parquetOExit")
		//check to see if it exists in the fs
		logDF(viewName,df)
		println(s"printing $viewName")
		df.show()
		df.createOrReplaceTempView(viewName)
	}
	
	
	def createViewFromParquetResolvedAllColumnSchemas(
			config: Config
			,spark: SparkSession
			,outputViewName: String
			,inputPaths: Seq[String]
			,inputTableNames: Seq[String]
			,col2RegenerateAsRowNum: String                   /* this is not yet implemented */
			,colsWithNonNullDefaults: Map[String,Any]
	){
//    val name=path.substring(path.lastIndexOf("/")+1)
		log.info("cme@ createViewFromParquetResolvedAllColumnSchemas inputTableNames has "+inputTableNames.length)
			
		val integerTypes      =Map  ( "ByteType"->0, "ShortType"-> 1, "IntegerType"->2, "LongType"-> 3)
		val integerTypeStrings=Array( "Byte"       , "Short"        , "Integer"        ,"Long")
		val realTypes      =Map  ("FloatType"->0,"DoubleType"->1)
		val realTypeStrings=Array("Float"       ,"Double")
		
		/* use LinkedHashMap to retain order in which columns are inserted */
		val dtypeSetsIn =scala.collection.mutable.LinkedHashMap[String,scala.collection.mutable.Set[String]]()
		val dtypeSetsOut=scala.collection.mutable.LinkedHashMap[String,String]()
		
		/* looking for columns and their corresponding datatypes.  
			* If the ._2 has only one datatype, we're ok
			* If the ._2 has more than one datatype, we have multiple conflicting datatypes thru the years.
			*/
			
		var dfab          = scala.collection.mutable.ArrayBuffer[DataFrame]()
		var subTableNames = scala.collection.mutable.ArrayBuffer[String]()
		var numTablesBeforeInputPathIteration=0
		for(path <- inputPaths){
			log.info("going for getFSListOfFiles on path="+path)
			var perYearTables=getArrayOfFileStatuses(config, path,false).filterNot(p=>p.getPath.getName=="_SUCCESS")
			log.info("perYearTables has length "+perYearTables.length)
			log.info("name="+perYearTables(0).getPath.getName)
			if(  (0<perYearTables.length)
					&& perYearTables(0).getPath.toString.endsWith("parquet")
				){
				log.info("do not have per year subdirectories")
				val fs = FileSystemManager.fileSystem(config)
				val name=perYearTables(0).getPath.toString
				val name0=name.substring(0,name.lastIndexOf("/"))
				val name1=name0.substring(0,name0.lastIndexOf("/"))
				log.info("\nname ="+name+"\nname0="+name0+"\nname1="+name1)
				perYearTables=fs.listStatus(new org.apache.hadoop.fs.Path(name1)).filter(f => f.getPath.toString.equals(name0))
			}
			/* "read" in the several years of data    Because I am taking no action, lazy grabs only the metadata*/
			var ii=0
			while(ii< perYearTables.length){
				//log.info("ii="+ii)
				log.info("about to read dfab "+perYearTables(ii).getPath )
				dfab+=spark.read.parquetOrExit(perYearTables(ii).getPath.toString, ViewFromParquetErrorCode)
				log.info("read went ok")
				subTableNames+=perYearTables(ii).getPath.toString.substring(1+perYearTables(ii).getPath.toString.lastIndexOf("/"))
				dfab(dfab.length-1).createOrReplaceTempView(subTableNames.last)
				/* */dfab(dfab.length-1).dtypes.foreach(d=>log.info(f"${d._1}%20s ${d._2}%15s")) 
				dfab(dfab.length-1).dtypes.foreach(f=> if(dtypeSetsIn.contains(f._1))dtypeSetsIn(f._1)+=f._2 else dtypeSetsIn+=((f._1,scala.collection.mutable.Set[String](f._2))))
				log.info("dtypes.lenth="+dfab(dfab.length-1).dtypes.length)
				/* after columns from first table are inserted, tack on any of the colsWIthNonNullDefaults which do not exist in the column list */ 
				if(0==ii){
					colsWithNonNullDefaults.foreach(f=>
						if(dtypeSetsIn.contains(f._1))
							dtypeSetsIn(f._1)+= f._2.getClass().getCanonicalName.substring(1+f._2.getClass().getCanonicalName.lastIndexOf("."))+"Type" 
						else 
							dtypeSetsIn+=((f._1,scala.collection.mutable.Set[String](
								f._2.getClass().getCanonicalName.substring(1+f._2.getClass().getCanonicalName.lastIndexOf("."))+"Type"
							)
						))
					)
				}
				ii+=1
			}
			for(dtypeSet<-dtypeSetsIn)log.info(f"dtypeSet=${dtypeSet._1}%30s ${dtypeSet._2}%s")
		}
		
		/* figure out if the columns have the same datatypes through all years, and if not, what should be the resolved datatype */
		var haveMultiTypeColumn=false
		log.info("     columnName  resolvedDataType   dataTypeIn0    dataTypeIn1   dataTypeIn2")  /* this meshes with 126 */
		for(dtypeSet<-dtypeSetsIn) {
			//log.info("dtypeSet="+dtypeSet._1+" "+dtypeSet._2)
			val aaa=dtypeSet._2
			if(1==dtypeSet._2.toSeq.length) {
				dtypeSetsOut+=((dtypeSet._1,dtypeSet._2.head.substring(0,dtypeSet._2.head.length-4)))
				//for((dso,jj)<-dtypeSetsOut.zipWithIndex)log.info(f"single add $jj%3d ${dso._1}%s ${dso._2}%s") 
			} else {
				if(dtypeSet._2.contains("StringType")){ 
						dtypeSetsOut+=((dtypeSet._1,"String"))
						//for((dso,jj)<-dtypeSetsOut.zipWithIndex)log.info(f"string add $jj%3d ${dso._1}%s ${dso._2}%s") 
				} else {
					val dts=dtypeSet._2
					//for(dt<-dts)log.info("dts ="+dt)
					var allIntegerTypes=true
					var highestIntegerType=0
					var allRealTypes=true
					var highestRealType=0
					for(dt<-dts){
						//log.info("dt="+dt)
						if(integerTypes.contains(dt)){
							if(highestIntegerType<integerTypes(dt)) highestIntegerType=integerTypes(dt)
						} else {  
							allIntegerTypes=false
						}
						if(realTypes.contains(dt)) {
							if(highestRealType<realTypes(dt)) highestRealType=realTypes(dt)
						} else {  
							allRealTypes=false
						}  
					}
					dtypeSetsOut+= ((dtypeSet._1,(
						if(allIntegerTypes) integerTypeStrings(highestIntegerType)
						else if(allRealTypes) realTypeStrings(highestRealType)
						else "String"
					)))    
					//for((dso,jj)<-dtypeSetsOut.zipWithIndex)log.info(f"mismatch   $jj%3d ${dso._1}%s ${dso._2}%s")
					haveMultiTypeColumn=true
				}
			}
		}
		val cols=scala.collection.mutable.ArrayBuffer[String]()
		for((dso,ii)<-dtypeSetsOut.zipWithIndex){
			val sb = new StringBuilder()
			for(dt<-dtypeSetsIn(dso._1))sb.append(f"$dt%15s")
			log.info(f"${dso._1}%-15s ${dso._2}%15s "+sb)
			cols+=dso._1
		}
		val sb = new StringBuilder()
		/* yes this next paragraph is a code block copy/pasted */
		var kk=0
		for(path <- inputPaths){
			log.info("going for getFSListOfFiles on path="+path)
			var perYearTables=getArrayOfFileStatuses(config, path,false).filterNot(p=>p.getPath.getName=="_SUCCESS")
			log.info("perYearTables has length "+perYearTables.length)
			log.info("name="+perYearTables(0).getPath.getName)
			if(  (0<perYearTables.length)
					&& perYearTables(0).getPath.toString.endsWith("parquet")
				){
				log.info("do not have per year subdirectories")
				val fs = FileSystemManager.fileSystem(config)
				val name=perYearTables(0).getPath.toString
				val name0=name.substring(0,name.lastIndexOf("/"))
				val name1=name0.substring(0,name0.lastIndexOf("/"))
				log.info("\nname ="+name+"\nname0="+name0+"\nname1="+name1)
				perYearTables=fs.listStatus(new org.apache.hadoop.fs.Path(name1)).filter(f => f.getPath.toString.equals(name0))
			}
			var ii=0
			log.info("perYearTables.length="+perYearTables.length)
			
			while(ii< perYearTables.length){
				//log.info("ii="+ii)
				sb.append((if((0<kk)||(0<ii))"union all " else "          ")+"select ")
				val dtypes= dfab(numTablesBeforeInputPathIteration+ii).dtypes
				//log.info("dtypes for ii="+ii+" "+ perYearTables(ii).getPath)
				//dtypes.foreach(d=>log.info(f"${d._1}%20s ${d._2}%15s"))
				val dtypesCols=dtypes.map(d=>d._1)
				var jj=0
				for(col<-cols){
					//log.info("checking col="+col)
					if(dtypesCols.contains(col)){
						//log.info("type compares "+dtypes(dtypesCols.indexOf(col))._2+" vs "+dtypeSetsOut(col)+"Type")
						if(dtypes(dtypesCols.indexOf(col))._2==(dtypeSetsOut(col)+"Type")) {
							if(col=="ccae_or_mdcr"){
								if(8< path.length){
									sb.append((if(0<jj)", "else"")+path.substring(path.length-8).substring(0,4))
								} else {
									sb.append((if(0<jj)", "else"")+col)
								}  
							} else {
								sb.append((if(0<jj)", "else"")+col)
								//log.info("see match to col="+col+"\n"+sb)
							}
						} else {
							sb.append((if(0<jj)","else"")+"cast("+col+" as "+dtypeSetsOut(col)+") as "+col+" ")
							//log.info("see need to cast col as "+dtypeSetsOut(col)+"\n"+sb)
						}
					} else
					if(colsWithNonNullDefaults.contains(col)){
						val singleQuoteIfNeeded= if(colsWithNonNullDefaults(col).getClass().getCanonicalName == "java.lang.String") "'" else ""
						sb.append((if(0<jj)", "else"")+singleQuoteIfNeeded+colsWithNonNullDefaults(col)+singleQuoteIfNeeded+" as "+col)
					}
					else {
						sb.append((if(0<jj)", "else"")+"cast(null as "+dtypeSetsOut.getOrElse(col,"String")+") as "+col)
						//log.info("do not see col in the dtypes\n"+sb)
					}  
					jj+=1  
				}
				sb.append(f" from "+subTableNames(numTablesBeforeInputPathIteration+ii)+" \n")
				//log.info("\n"+sb)
				ii+=1
			}
			numTablesBeforeInputPathIteration+=ii
			kk+=1
		}
		log.info("\"union all\"   sql=\n"+sb)
		log.info("about to submit sql\n"+sb)
		val df=spark.sql(sb.toString())
		df.createOrReplaceTempView(outputViewName)
	}
// 	def createViewFromParquetMultiSchemaInstances(
//		config: Config
//		,spark: SparkSession
//		,path: String
//		,viewName: String
//		,comaSeparatedListOfColumns : String
//		,vendorSchemaMap: SchemaParser.SchemaMap = null
//		,output_nppk: Boolean = false
//		): Unit = {
//		log.info("cme@ createViewFromParquetMultiSchemaInstances with viewName="+viewName)
//		//val dfccae = spark.read.parquetOrExit(path, ViewFromParquetErrorCode)
//		val name=path.substring(path.lastIndexOf("/")+1)
//		log.info("name="+name)
//			
//		val integerTypes      =Map  ( "ByteType"->0, "ShortType"-> 1, "IntegerType"->2, "LongType"-> 3)
//		val integerTypeStrings=Array( "Byte"       , "Short"        , "Integer"        ,"Long")
//		val realTypes         =Map  ("FloatType"->0,"DoubleType"->1)
//		val realTypeStrings=Array("Float"       ,"Double")
//		val dtypeSetsIn =scala.collection.mutable.Map[String,scala.collection.mutable.Set[String]]()
//		val dtypeSetsOut=scala.collection.mutable.Map[String,String]() 
//		val cols=comaSeparatedListOfColumns.split(",").map(s=>s.trim)
//		
//		/* looking for columns and their corresponding datatypes.  
//			* If the ._2 has only one datatype, we're ok
//			* If the ._2 has more than one datatype, we have multiple conflicting datatypes thru the years.
//			*/
//			
//		var dfab          = scala.collection.mutable.ArrayBuffer[DataFrame]()
//		var subTableNames = scala.collection.mutable.ArrayBuffer[String]()
//		log.info("going for getFSListOfFiles on path="+path)
//		var perYearTables=getArrayOfFileStatuses(config, path).filterNot(p=>p.getPath.getName=="_SUCCESS")
//		log.info("perYearTables has length "+perYearTables.length)
//		log.info("name="+perYearTables(0).getPath.getName)
//		if(  (0<perYearTables.length)
//				&& perYearTables(0).getPath.toString.endsWith("parquet")
//			){
//			log.info("do not have per year subdirectories")
//			val fs = FileSystemManager.fileSystem(config)
//			val name=perYearTables(0).getPath.toString
//			val name0=name.substring(0,name.lastIndexOf("/"))
//			val name1=name0.substring(0,name0.lastIndexOf("/"))
//			log.info("\nname ="+name+"\nname0="+name0+"\nname1="+name1)
//			perYearTables=fs.listStatus(new org.apache.hadoop.fs.Path(name1)).filter(f => f.getPath.toString.equals(name0))
//		}
//		/* tell it to read in the several years of data, but know that lazy execution will not do the actual read yet */
//		var ii=0
//		while(ii< perYearTables.length){
//			//log.info("ii="+ii)
//			log.info("about to read dfab "+perYearTables(ii).getPath )
//			dfab+=spark.read.parquetOrExit(perYearTables(ii).getPath.toString, ViewFromParquetErrorCode)
//			//.limit(10)
//			//log.info(showString(dfab.last))
//			subTableNames+=perYearTables(ii).getPath.toString.substring(1+perYearTables(ii).getPath.toString.lastIndexOf("/"))
//			dfab(ii).createOrReplaceTempView(subTableNames.last)
//			//dfab(dfab.length-1).dtypes.foreach(d=>log.info(f"${d._1}%20s ${d._2}%15s")) 
//			dfab(dfab.length-1).dtypes.foreach(f=> if(dtypeSetsIn.contains(f._1.toLowerCase))dtypeSetsIn(f._1.toLowerCase())+=f._2 else dtypeSetsIn+=((f._1.toLowerCase(),scala.collection.mutable.Set[String](f._2))))
//			ii+=1
//		}
//		for(dtypeSet<-dtypeSetsIn)log.info(f"dtypeSet=${dtypeSet._1}%30s ${dtypeSet._2}%s")
//		
//		/* figure out if the columns have the same datatypes through all years, and if not, what should be the resolved datatype */
//		/* in the event that the vendorSchema is present (is not the default null, go with that no matter what the input datatypes are */
//		val sbSql = new StringBuilder()
//		var haveMultiTypeColumn=false
//		if(null==vendorSchemaMap){
//			for(dtypeSet<-dtypeSetsIn) {
//				//log.info("dtypeSet="+dtypeSet._1+" "+dtypeSet._2+" which has length "+dtypeSet._2.toSeq.length)
//				if(1==dtypeSet._2.toSeq.length) {
//					dtypeSetsOut+=((dtypeSet._1.toLowerCase,dtypeSet._2.head.substring(0,dtypeSet._2.head.length-4)))
//					//for((dso,jj)<-dtypeSetsOut.zipWithIndex)log.info(f"single add $jj%3d ${dso._1}%s ${dso._2}%s") 
//				} else {
//					if(dtypeSet._2.contains("StringType")){ 
//							dtypeSetsOut+=((dtypeSet._1,"String"))
//							//for((dso,jj)<-dtypeSetsOut.zipWithIndex)log.info(f"string add $jj%3d ${dso._1}%s ${dso._2}%s") 
//					} else {
//						val dts=dtypeSet._2
//						//for(dt<-dts)log.info("dts ="+dt)
//						var allIntegerTypes=true
//						var highestIntegerType=0
//						var allRealTypes=true
//						var highestRealType=0
//						for(dt<-dts){
//							//log.info("dt="+dt)
//							if(integerTypes.contains(dt)){
//								if(highestIntegerType<integerTypes(dt)) highestIntegerType=integerTypes(dt)
//							} else {  
//								allIntegerTypes=false
//							}
//							if(realTypes.contains(dt)) {
//								if(highestRealType<realTypes(dt)) highestRealType=realTypes(dt)
//							} else {  
//								allRealTypes=false
//							}  
//						}
//						dtypeSetsOut+= ((dtypeSet._1,(
//							if(allIntegerTypes) integerTypeStrings(highestIntegerType)
//							else if(allRealTypes) realTypeStrings(highestRealType)
//							else "StringType"
//						)))    
//						//for((dso,jj)<-dtypeSetsOut.zipWithIndex)log.info(f"mismatch   $jj%3d ${dso._1}%s ${dso._2}%s")
//						haveMultiTypeColumn=true
//					}
//				}
//			}
//			log.info("     columnName  resolvedDataType   dataTypeIn0    dataTypeIn1   dataTypeIn2")  /* this meshes as the hwith 126 */
//			for((dso,ii)<-dtypeSetsOut.zipWithIndex){
//				val sb = new StringBuilder()
//				for(dt<-dtypeSetsIn(dso._1))sb.append(f"$dt%15s")
//				log.info(f"${dso._1}%-15s ${dso._2}%15s "+sb)
//			}
//			ii=0
//			log.info("perYearTables.length="+perYearTables.length)
//			while(ii< perYearTables.length){
//				//log.info("ii="+ii)
//				sbSql.append((if(0<ii)"union all " else "          ")+"select ")
//				val dtypes= dfab(ii).dtypes
//				//log.info("dtypes for ii="+ii+" "+ perYearTables(ii).getPath)
//				//dtypes.foreach(d=>log.info(f"${d._1}%20s ${d._2}%15s"))
//				val dtypesCols=dtypes.map(d=>d._1.toLowerCase())
//				//dtypesCols.foreach(f=>log.info("dtypesCols "+f))
//				var jj=0
//				for(col<-cols){
//					//log.info("checking col="+col)
//					if(dtypesCols.contains(col.toLowerCase)){
//						//log.info("type compares "+dtypes(dtypesCols.indexOf(col))._2+" vs "+dtypeSetsOut(col)+"Type")
//						if(dtypes(dtypesCols.indexOf(col))._2==(dtypeSetsOut(col)+"Type")) {
//							sbSql.append((if(0<jj)", "else"")+col)
//							//log.info("see match to col="+col+"\n"+sb)
//						} else {
//							sbSql.append((if(0<jj)","else"")+"cast("+col+" as "+dtypeSetsOut(col)+") as "+col+" ")
//							//log.info("see need to cast col as "+dtypeSetsOut(col)+"\n"+sb)
//						}
//					} else {
//						sbSql.append((if(0<jj)", "else"")+"cast(null as "+dtypeSetsOut.getOrElse(col,"String")+") as "+col)
//						//log.info("do not see col in the dtypes\n"+sb)
//					}  
//					jj+=1  
//				}
//				sbSql.append(f" from "+subTableNames(ii)+" \n")
//				//log.info("\n"+sb)
//				ii+=1
//			}
//		} else {
//			log.info("resolved datatypes from schema file, not the parquet's")
//			import org.apache.spark.sql.types._
//			var schemaMap : Map[String, StructField]=null
//			for((vsm,ii)<-vendorSchemaMap.zipWithIndex){
//				//var msg="vsm._1._1="+(if(null==vsm._1._1)"null" else vsm._1._1.toLowerCase)
//				//println(msg);log.info(msg)
//				//msg="vsm._1._2="+(if(null==vsm._1._2)"null" else vsm._1._2.toLowerCase)
//				//println(msg);log.info(msg)
//				val tableName=vsm._1._1.toLowerCase()
//				val dataSet  =vsm._1._2.toLowerCase()
//				if(  (tableName==viewName.toLowerCase())
//						&&(dataSet=="production_multi_year")
//					){
//						schemaMap=vsm._2
//				}
//			}
//			if(null==schemaMap){
//				val msg="was not able to match "+viewName+" and dataSet  \nproduction_multi_year\n" 
//				log.info(msg);println(msg)
//				System.exit(4)
//			} else {
//				val msg="yes able to match "+viewName+" and dataSet  \nproduction_multi_year\n" 
//				log.info(msg);println(msg)
//			}
//			
//			val sb = new StringBuilder()
//			var msg=""
//			val yearStart=2001
//			var yearTableIndex=0
//			log.info("perYearTables.length="+perYearTables.length)
//			sbSql.clear
//			sbSql.append("with tableWad as (\n")   /* this was prepping to do a row_number but that is WAY slow.   second pass is to use monotonically_increasing_id */
//			while(yearTableIndex< perYearTables.length){
//				/**/log.info("yearTable="+subTableNames(yearTableIndex))
//				val dtypes= dfab(yearTableIndex).dtypes
//				//log.info("dtypes for ii="+ii+" "+ perYearTables(ii).getPath)
//				//dtypes.foreach(d=>log.info(f"${d._1}%20s ${d._2}%15s"))
//				val dtypesCols=dtypes.map(d=>d._1.toLowerCase())
//				//dtypesCols.foreach(f=>log.info("dtypesCols "+f))
//				sbSql.append((if(0<yearTableIndex)"union all " else "          ")+"select ")
//				for((col,jj)<-cols.zipWithIndex){
//					//msg="starting on "+subTableNames(yearTableIndex)+" col="+col;println(msg);log.info(msg)
//					if(0==col.length){msg="AAAHHH have a zero length column from the input comaSeparatedListOfColumns\n"+comaSeparatedListOfColumns+"\nThis is fatal";println(msg);log.fatal(msg);System.exit(45)}
//					var gotMatch=false
//					var inputDataType=""
//					var imposedDataType=""
//					var comment=""
//					schemaMap.foreach{f=>
//						if(f._1==col){
//							comment=if(f._2.metadata.contains("comment"))f._2.metadata.getString("comment") else ""
//							val dType=f._2.dataType.toString()
//							inputDataType=dType.substring(0,dType.length-4)
//							//log.info(f"f._1=${f._1}%20s f._2=${f._2}%45s dType=$dType%-12s imposedDataType=$inputDataType%10s comment=$comment%s")
//							//log.info("meta="+f._2.metadata.toString())
//						}
//					}
//					//log.info("col="+col+" from the schema.csv file iputDataType="+inputDataType)
//					if(0==inputDataType.length){msg="have no dataType from schema file for col="+col+" this is fatal";println(msg);log.fatal(msg);System.exit(76)}
//					var colIn=col
//					var colOut=col
//					
//					if(dtypesCols.contains(col)){
//						//log.info("col match on "+col)
//						var didSqlEntry=false
//						if(col=="enrolid"){
//							sbSql.append((if(0<jj)" ,"else"")+"(cast(enrolid as Long)+100000000000000L) as person_id")
//							colOut="person_id"
//							imposedDataType="Long"
//							didSqlEntry=true
//						} else
//						if(comment=="dollars and cents with decimal point") {
//							sbSql.append((if(0<jj)" ,"else"")+"round("+col+",2) as "+col)
//							imposedDataType="Double rounded to two digits right of decimal"
//							didSqlEntry=true
//						} else
//						if("Date"==inputDataType){
//							sbSql.append((if(0<jj)" ,"else"")+"cast("+col+" as Timestamp) as "+col)
//							imposedDataType="Timestamp"
//							didSqlEntry=true
//						} else 
//						if("dxver"==col){
//							sbSql.append((if(0<jj)" ,"else"")+"cast(case when dxver is null then '9' else dxver end as string) as "+col)
//							log.info(f"${yearStart+yearTableIndex}%d                    n/a $col%20s  $inputDataType%-14s $inputDataType%s")
//							imposedDataType=inputDataType
//							didSqlEntry=true
//						}
//						for((dtypeSet,ii)<-dtypeSetsIn.zipWithIndex) {
//							//log.info("dtypeSet="+dtypeSet._1+" "+dtypeSet._2)
//							if(col==dtypeSet._1) {
//								//log.info("dtypeSet._2.size="+dtypeSet._2.size+" inputDataType="+inputDataType+" dtypeSet._2(0)="+dtypeSet._2.toSeq(0))
//								if(false==didSqlEntry){
//									if(  (1==dtypeSet._2.size)
//											&&((inputDataType+"Type")==dtypeSet._2.toSeq(0).toString)
//										){
//										log.info("this column had only one datatype incomming, which matched the desired type col="+col)
//										sbSql.append((if(0<jj)" ,"else"")+col)
//									} else {
//										log.info("need to recast the type of "+col)
//										sbSql.append((if(0<jj)" ,"else"")+"cast("+col+" as "+inputDataType+") as "+col)
//										imposedDataType=inputDataType
//									}
//								}
//								//log.info(f"match to dtype $ii%3d ")
//								val miniSb=new StringBuilder()		
//								for(dt<-dtypeSet._2)miniSb.append(f"$dt%15s")
//								log.info(f"${yearStart+yearTableIndex}%d $colIn%20s $colOut%20s $inputDataType%-14s $imposedDataType%-40s $miniSb%s")
//							}
//						}
//					} else {
//						log.info("have null because false==dtypesCols.contains("+col+")")
//						if("dxver"==col){
//							sbSql.append((if(0<jj)" ,"else"")+" '9' as "+col)
//							log.info(f"${yearStart+yearTableIndex}%d                    n/a $col%20s  $inputDataType%-14s $inputDataType%s")
//							imposedDataType=inputDataType
//						} else 
//						if("Date"==inputDataType){
//							sbSql.append((if(0<jj)" ,"else"")+"cast(null  as Timestamp) as "+col)
//							imposedDataType="Timestamp"
//						} else {
//							sbSql.append((if(0<jj)" ,"else"")+"cast(null as "+inputDataType+") as "+col)
//							log.info(f"${yearStart+yearTableIndex}%d                    n/a $col%20s  $inputDataType%-14s $inputDataType%s")
//							imposedDataType=inputDataType
//						}	
//					}
//				}
//				sbSql.append(f" from "+subTableNames(yearTableIndex)+" where coalesce(enrolid,'') != '' \n")
//				yearTableIndex+=1
//			}
//		}
//		
//		sbSql.append(") select (monotonically_increasing_id() ")
//		if(viewName=="outpatient_services")sbSql.append("+1000000000000 ")
//		sbSql.append(") as seqnum, t.* from tableWad t")     /*  when this was row_number, it this just died (30 min of no output vs 4 min complete */
//		
//		log.info("\"union all\"   sql=\n"+sbSql)
//
//		/* this was used to confirm that 2015 and 2016 had 3Million null dxvers, and this case pushes them to be "9" */		
////		sbSql.clear
////		sbSql.append("with pre as ( \n")
////		sbSql.append("          select case when dxver is null then '9' else dxver end as dxver from outpatient_services_ccae_2015  \n")
////		sbSql.append("union all select case when dxver is null then '9' else dxver end as dxver from outpatient_services_ccae_2016  \n")
////		sbSql.append("union all select case when dxver is null then '9' else dxver end as dxver from outpatient_services_mdcr_2015  \n")
////		sbSql.append("union all select case when dxver is null then '9' else dxver end as dxver from outpatient_services_mdcr_2016  \n")
////		sbSql.append(") \n")
////		sbSql.append("select dxver, count(dxver) from pre group by dxver \n")
//		
//		log.info("about to submit sql\n"+sbSql)
//		val df=spark.sql(sbSql.toString())		
//		df.createOrReplaceTempView(viewName)
//		log.info("df.count="+df.count)
//		log.info(showString(df,20,80))
//		
//		if(output_nppk){
//			val outFid0=perYearTables(0).getPath.toString()
//			val outFid1=outFid0.substring(outFid0.indexOf(":")+1)
//			val outFid2=outFid1.substring(outFid1.indexOf(":")+1)
//			val outFid3=outFid2.substring(outFid2.indexOf("/"),outFid2.lastIndexOf("/")).replace("/raw/","/intermediate/")
//			log.info("outfid0="+outFid0)
//			log.info("outfid1="+outFid1)
//			log.info("outfid2="+outFid2)
//			log.info("outfid3="+outFid3)
//			log.info("about to attempt parquet write on "+outFid3+"_nppk")
//			df.write.option("compression","gzip").parquet(outFid3+"_nppk")
//		}
//		log.info("atEndOf createViewFromParquetMultiSchemaInstances")
////    System.exit(444)
//	} 

	def createDataFrameFromSQL(spark: SparkSession, sql: String,sqlFileName: String): DataFrame = {
		val df = spark.sqlOrExit(sql, DataFrameFromSQLErrorCode)
		logDF(sqlFileName,df)
		df
	}
	
	private def logDF(descriptor: String, df: DataFrame) = log.info(descriptor+MiscellaneousUtils.showString(df,20,20))
}
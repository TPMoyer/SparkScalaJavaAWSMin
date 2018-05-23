package myScalaUtils

import org.apache.log4j.Logger

import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer
//import org.apache.log4j.{Level, Logger, LogManager, PropertyConfigurator}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.sqlContext.implicits._
import org.apache.commons.lang3.StringUtils
import java.text.SimpleDateFormat
import com.typesafe.config.Config
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import myScalaUtils.FileIO._
import myScalaUtils.MiscellaneousUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,LocalFileSystem,RawLocalFileSystem,Path,FileStatus}
//import java.io.File
import sys.process._
import org.apache.commons.lang3.exception.ExceptionUtils

object MiscellaneousUtils  {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("MiscellaneousUtils")
  
  def tryCatchFinally(){
    import org.apache.commons.lang3.exception.ExceptionUtils
    var retVal=0
    val someValue = "myAppName"
    try {
      val aVal="123-456"
    } catch {
      case e: Exception =>
        retVal=1
        log.error("an exception occurred under  "+someValue+":\n" + ExceptionUtils.getStackTrace(e))
        println("\n\n*************************************************Failed with exception *******************************************\n")
        throw new Exception("Spark Error")
      case unknown : Throwable =>
        retVal=2
        log.error("an exception occurred under  "+someValue+":\n" + ExceptionUtils.getStackTrace(unknown))
        println("\n\n*************************************************Failed with unknown type exception *******************************************\n")
        throw new Exception("Spark Error")      
    } finally {
      /* some final act or acts */
      log.debug("retval="+retVal)
    }
  }
  def  showHiveDatabases(
    spark      : SparkSession
  ){
    log.info("cme@ showHiveDatabases")
    val df=spark.sql("show databases")
    if(1==df.count){
      log.info("if this shows only default, the likely situation is that you are pointing to a self generated warehouse in   System.getProperty(\"user.dir\")/spark-warehouse ")
      log.info("on HDP systems check Ambari... hosts... hive... config.... and scroll down to  hive.metastore.warehouse.dir ")
      log.info("As of spark2.X this location needs to be set into spark.sql.warehouse.dir=hdfs///apps/hive/warehouse")    
      log.info("this is most easily done as a -Doption=value   for example    -Dspark.sql.warehouse.dir=hdfs///apps/hive/warehouse ")
    }
    log.info("show databases row count="+df.count+"\n"+showString(df))
  }
  def setSample(
    //config     : Config, 
    spark      : SparkSession,
    fs         : FileSystem,
    lfs        : LocalFileSystem,
    useLocalFs : Boolean,
    source2TargetRow : scala.collection.mutable.Map[String,String],
    counterAct : Integer,
    fid : String,
    lastPriorSrcFileSystemType : String
  ) : String={
    log.info("cme@setSample on "+fid+" lastPriorSrcFileSystemType="+lastPriorSrcFileSystemType)
    val inFid=fid
    val sampleDid=fid.substring(0,fid.lastIndexOf("/"))+"/sample"
    var exists=  (if(useLocalFs)lfs else fs).exists(new Path(sampleDid))
    if(!exists){
      log.info("see sample directory as non pre-existing.   Kicking off mkdir("+sampleDid+")")
      (if(useLocalFs)lfs else fs).mkdirs(new Path(sampleDid))
    }
    val tgtFid=sampleDid+fid.substring(fid.lastIndexOf("/"))
    val tgtPath=new Path(tgtFid)
    exists=(if(useLocalFs)lfs else fs).exists(tgtPath)
    if(exists){
      log.info("see file as pre-existing.   Kicking off delete")
      (if(useLocalFs)lfs else fs).delete(tgtPath,false)
    }
    var cmd= ""
    if("hdfs"==lastPriorSrcFileSystemType){
      var rows = spark.read.textFile(inFid).take(source2TargetRow(f"$counterAct%02d sampleNumberOfRows").toInt)
      val sb=new StringBuilder()
      for((r,ii)<-rows.zipWithIndex){
        sb.append(r+"\n")
        //log.info(f"$ii%3d $r%s")
      }
      fileO(fs,lfs,useLocalFs,tgtFid,sb)
    } else {
      //cmd="head -n "+ source2TargetRow(f"$counterAct%02d sampleNumberOfRows")+" "+ fid+" > "+tgtFid
      cmd="head -n "+ source2TargetRow(f"$counterAct%02d sampleNumberOfRows") +" "+ fid
      if(System.getProperty("os.name").startsWith("Win")){
        cmd=cmd.replace("/","\\")
      }
      val cmd0="mkdir -p "+sampleDid
      log.info("about to kick off extermal cmd0=\n"+cmd0)
      val result =cmd0.!!
      log.info("return set ="+result)
      var sb=new StringBuilder()
      sb.append(cmd.!!)
      //log.info("return set ="+sb)
      fileO(fs,lfs,useLocalFs,tgtFid,sb)
    }
    tgtFid
  }

  def recommendNumberOfParquetPartitions(
    spark      : SparkSession,
    fs         : FileSystem,
    lfs        : LocalFileSystem,
    useLocalFs : Boolean,
    df : DataFrame,
    fidOut: String
  ):Int={
    log.info("df.count()="+df.count())
    var msg = ""
    /* from https://spark.apache.org/docs/2.0.2/api/scala/index.html#org.apache.spark.sql.types.package
     *  BinaryType           extends BinaryType           with Product with Serializable
     *  BooleanType          extends BooleanType          with Product with Serializable
     *  ByteType             extends ByteType             with Product with Serializable
     *  CalendarIntervalType extends CalendarIntervalType with Product with Serializable
     *  DateType             extends DateType             with Product with Serializable
     *  DoubleType           extends DoubleType           with Product with Serializable
     *  FloatType            extends FloatType            with Product with Serializable
     *  IntegerType          extends IntegerType          with Product with Serializable
     *  LongType             extends LongType             with Product with Serializable
     *  NullType             extends NullType             with Product with Serializable
     *  ShortType            extends ShortType            with Product with Serializable
     *  StringType           extends StringType           with Product with Serializable
     *  TimestampType        extends TimestampType        with Product with Serializable
     *  
     *  ArrayType            extends AbstractDataType     with Serializable
     *  MapType              extends AbstractDataType     with Serializable
     *  StructType           extends AbstractDataType     with Serializable
     */
    val dfTypes=df.dtypes
    val sfa=new ArrayBuffer[StructField]
    for((d,ii)<-dfTypes.zipWithIndex){
      log.info(f"$ii%3d ${d._1}%30s ${d._2}%s")
      d._2 match {
        case "BinaryType"           =>sfa+=StructField(d._1,BinaryType          )
        case "BooleanType"          =>sfa+=StructField(d._1,BooleanType         )
        case "ByteType"             =>sfa+=StructField(d._1,ByteType            )
        case "CalendarIntervalType" =>sfa+=StructField(d._1,CalendarIntervalType)
        case "DateType"             =>sfa+=StructField(d._1,DateType            )
        case "DoubleType"           =>sfa+=StructField(d._1,DoubleType          )
        case "FloatType"            =>sfa+=StructField(d._1,FloatType           )
        case "IntegerType"          =>sfa+=StructField(d._1,IntegerType         )
        case "LongType"             =>sfa+=StructField(d._1,LongType            )
        case "NullType"             =>sfa+=StructField(d._1,NullType            )
        case "ShortType"            =>sfa+=StructField(d._1,ShortType           )
        case "StringType"           =>sfa+=StructField(d._1,StringType          )
        case "TimestampType"        =>sfa+=StructField(d._1,TimestampType       )
//        case "ArrayType"            =>sfa+=StructField(d._1,ArrayType           )
//        case "MapType"              =>sfa+=StructField(d._1,MapType             )
//        case "StructType"           =>sfa+=StructField(d._1,StructType          )
        case whoa  => msg="95 miscellaneousUtils Unexpected case from d._2="+d._2+": " + whoa.toString;println(msg);log.warn(msg)
      }
    }
    val rdd=df.rdd
    val rddNumPartitions=rdd.partitions.size
    log.info("rdd has "+rddNumPartitions+" partitions")
//    if(1==rddNumPartitions)return(1)
    /* cleave off one partition, and write that to disk, to see how many partitions to set for the full thing */
    val rdd1=rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter else iter.drop(iter.length)} /* take partition #0, drop all rows in other partitions */ 
    val dftest=spark.sqlContext.createDataFrame(rdd1.repartition(1),StructType(sfa.toList)) /* make a single partition and convert back to dataframe */
    log.info("about to attempt deleteRecursively on "+fidOut+"_test")
    
    /* For Local (Non Distributed File System on an HDFS cluster, had bivariant behavior. 
     * Some file IO needed file:// prepended, 
     * some required it not be present.
     */
     (if(useLocalFs)lfs else fs).delete(new Path(fidOut+"_test"),true)
//    var fidOutA=fidOut.substring(7)
//    if(fidOut.startsWith("file://")){ 
//      log.info("2'nd try without the file://   about to attempt deleteRecursively on "+fidOutA+"_test")
//      deleteRecursively( new File(fidOutA+"_test"))
//    }
    
    log.info("about to attempt parquet write on "+fidOut+"_test"+" from "+dftest.rdd.partitions.size+" partitions")
    dftest.write.option("compression","gzip").parquet(fidOut+"_test")
    /* bit odd behavior on Local on a hadoop cluster, some io requires the file:// on the beginning, others require it to not be there */
    var loff=getArrayOfFileStatuses(if(useLocalFs)lfs else fs,fidOut+"_test",true).filter(f => f.getPath.toString.endsWith(".gz.parquet"))
    
    val singlePartitionSize=loff(0).getLen
    var desiredNumberOfPartitions= (0.5+(rddNumPartitions*singlePartitionSize/256000000.0)).toInt
    if(0== desiredNumberOfPartitions) desiredNumberOfPartitions+=1
    log.info("desiredNumberOfPartitions ="+desiredNumberOfPartitions+" from "+singlePartitionSize)
    (if(useLocalFs)lfs else fs).delete(new Path(fidOut+"_test"),true)
    
    desiredNumberOfPartitions
  }
  def prettyReNameMROutput(
    config : Config,
    fs     : FileSystem,
    lfs    : LocalFileSystem,
    useLocalFs : Boolean,
    path   : String
  ){
    var loff=getArrayOfFileStatuses(if(useLocalFs)lfs else fs,path,true)
 
    log.info("loff.length="+loff.length)
    for((f,ii)<-loff.zipWithIndex)(log.info(f"loff $ii%3d ${f.getLen}%9d ${f.getPath}%s "))
    val lof=loff.filter(p => p.getPath.toString().contains("/part-"))
    log.info("lof.length="+lof.length)
    for((f,ii)<-lof.zipWithIndex)(log.info(f"lof  $ii%3d ${f.getLen}%9d ${f.getPath}%s "))
    
    /* on the outside of the cluster the starting portion of a parquet name is part-r-00000  vs inside the cluster it is part-00000
     * This cause the code change multiple times below... what had been parts(2)     became    if("r"==parts(1))2 else 1
     *  */
    
    var f=lof(lof.length-1)
    log.info("ok0")
    val name=f.getPath.toString()
    log.info("ok1 name="+name)
    val pos0=name.lastIndexOf("part-")
    log.info("pos0="+pos0)
    //val bod=name.substring(0,pos0-(1+tackOn2Parquet.length))
    val bod=name
    val keeper=bod.substring(bod.lastIndexOf("/"))
    val part=name.substring(pos0)
    val parts=part.split("-")
    log.info("parts has "+parts.length+" parts")
    log.info(f"lof  last   pos0=$pos0%3d ${f.getLen}%9d bod=$bod%s keeper=$keeper%s part=$part%s ${f.getPath}%s ")
    for((p,jj)<-parts.zipWithIndex)(log.info(f"         $jj%d $p%s"))
    import scala.math.log10
    val size=(if("00000"==parts(if("r"==parts(1))2 else 1))1 else log10(parts(if("r"==parts(1))2 else 1).toInt.toDouble).toInt+1)
    log.info("size="+size)
    
    for((f,ii)<-lof.zipWithIndex)({
      val path=f.getPath.toString()
      log.info("name="+path)
      val pos0=path.lastIndexOf("part-")
      //val bod=name.substring(0,pos0-(1+tackOn2Parquet.length))
      val bod=path.substring(0,pos0-1)
      val keeper="/"+bod.substring(bod.lastIndexOf("/")+1)
      val part=path.substring(pos0)
      val parts=part.split("-")
      log.info(f"lof  $ii%3d pos0=$pos0%3d ${f.getLen}%9d bod=$bod%s keeper=$keeper%s part=$part%s ${f.getPath}%s ")
      for((p,jj)<-parts.zipWithIndex)(log.info(f"     $ii%3d $jj%d $p%s"))
      log.info("parts(if(\"r\"==parts(1))2 else 1).length="+parts(if("r"==parts(1))2 else 1).length+" size="+size+" parts(if(\"r\"==parts(1))2 else 1).length-size="+(parts(if("r"==parts(1))2 else 1).length-size)) 
      val reName0="-"+ (if(1==lof.length)"" else parts(if("r"==parts(1))2 else 1).substring(parts(if("r"==parts(1))2 else 1).length-size).toString())
      /* */log.info("reName0="+reName0)
      val form="in%0"+f"$size%d"+"dto%"+f"$size%dd"
      log.info("form="+form)  
      val reName1=if(1==lof.length)"single" else  form.format(0,lof.length-1)
      val reName2=parts(parts.length-1).substring(parts(parts.length-1).indexOf("."))   
      log.info("\n    bod="+bod+"\n keeper="+keeper+"\nreName0="+reName0+"\nreName1="+reName1+"\nreName2="+reName2)
      //val newName=bod+tackOn2Parquet+keeper+reName0+reName1+reName2
      val newName=(bod+keeper+reName0+reName1+reName2).toLowerCase()
      log.info("newName="+newName)
      (if(useLocalFs)lfs else fs).rename(f.getPath,new Path(newName))
    })
  }
  def logFSStuff(
     fs   :FileSystem
    ,rlfs : RawLocalFileSystem
    ,lfs  : LocalFileSystem
    ,conf : Configuration
    ){
    import java.text.NumberFormat
    var msg=""
    val fsStatus=fs.getStatus
    val sd=fs.getServerDefaults
    msg=f"\n\nfs default FileSystem\n Capacity=${NumberFormat.getNumberInstance.format(fsStatus.getCapacity)}%20s"+
        f"\nRemaining=${NumberFormat.getNumberInstance.format(fsStatus.getRemaining)}%20s"+
        f"\n     Used=${NumberFormat.getNumberInstance.format(fsStatus.getUsed)}%20s"+ 
        "\nscheme="+fs.getScheme+
        "\nworkingDirectory="+fs.getWorkingDirectory+
        "\nFileSystem="+fs.getHomeDirectory.getFileSystem(conf)+
        "\nhomeDirectory="+fs.getHomeDirectory.toString+
        "\nBlockSize="+sd.getBlockSize+
        "\nBytesPerChecksum="+sd.getBytesPerChecksum+
        "\nChecksumType="+sd.getChecksumType+
        "\nEncryptDataTransfer="+sd.getEncryptDataTransfer+
        "\nFileBufferSize="+sd.getFileBufferSize+
        "\nReplication="+sd.getReplication+
        "\nTrashInterval="+sd.getTrashInterval+
        "\nWritePacketSize="+sd.getWritePacketSize+
        "\n"
    log.info(msg)
    val rlfsStatus= rlfs.getStatus()
    msg=f"\n\nrlfs RawLocalFileSystem\n Capacity=${NumberFormat.getNumberInstance.format(rlfsStatus.getCapacity)}%20s"+
        f"\nRemaining=${NumberFormat.getNumberInstance.format(rlfsStatus.getRemaining)}%20s"+
        f"\n     Used=${NumberFormat.getNumberInstance.format(rlfsStatus.getUsed)}%20s"+
       "\nworkingDirectory="+rlfs.getWorkingDirectory+
       "\nFileSystem="+rlfs.getHomeDirectory.getFileSystem(conf)+
       "\nhomeDirectory="+rlfs.getHomeDirectory.toString+"\n"
    log.info(msg)
    val lfsStatus : org.apache.hadoop.fs.FsStatus=lfs.getStatus
    msg=f"\n\nlfs LocalFileSystem\n Capacity=${NumberFormat.getNumberInstance.format(lfsStatus.getCapacity)}%20s"+
        f"\nRemaining=${NumberFormat.getNumberInstance.format(lfsStatus.getRemaining)}%20s"+
        f"\n     Used=${NumberFormat.getNumberInstance.format(lfsStatus.getUsed)}%20s"+ 
        "\nscheme="+lfs.getScheme+"\nworkingDirectory="+lfs.getWorkingDirectory+"\nFileSystem="+lfs.getHomeDirectory.getFileSystem(conf)+
        "\nhomeDirectory="+lfs.getHomeDirectory.toString+"\n"
   log.info(msg) 
  }
   /**
  * Compose the string representing rows for output
  *
  * @param _numRows Number of rows to show
  * @param truncate If set to more than 0, truncates strings to `truncate` characters and
  *                   all cells will be aligned right.
  */
  def showString(
    df:DataFrame
    ,_numRows: Int = 20
    ,truncateWidth: Int = 20
  ): String = {
    val numRows = _numRows.max(0)
    val takeResult = df.take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    val rows: Seq[Seq[String]] = df.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
      val str = cell match {
        case null => "null"
        case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
        case array: Array[_] => array.mkString("[", ", ", "]")
        case seq: Seq[_] => seq.mkString("[", ", ", "]")
        case _ => cell.toString
      }
      if (truncateWidth > 0 && str.length > truncateWidth) {
        // do not show ellipses for strings shorter than 4 characters.
        if (truncateWidth < 4) str.substring(0, truncateWidth)
        else str.substring(0, truncateWidth - 3) + "..."
      } else {
        str
      }
    }: Seq[String]
  }
  
  val sb = new StringBuilder
  val numCols = df.schema.fieldNames.length
  
  // Initialise the width of each column to a minimum value of '3'
  val colWidths = Array.fill(numCols)(3)
  
  // Compute the width of each column
  for (row <- rows) {
    for ((cell, i) <- row.zipWithIndex) {
      colWidths(i) = math.max(colWidths(i), cell.length)
    }
  }
  
  // Create SeparateLine
  val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()
  
  // column names
  rows.head.zipWithIndex.map { case (cell, i) =>
  if (truncateWidth > 0) {
      StringUtils.leftPad(cell, colWidths(i))
    } else {
      StringUtils.rightPad(cell, colWidths(i))
    }
  }.addString(sb, "|", "|", "|\n")
  
  sb.append(sep)
  
  // data
  rows.tail.map {
    _.zipWithIndex.map { case (cell, i) =>
      if (truncateWidth > 0) {
         StringUtils.leftPad(cell.toString, colWidths(i))
      } else {
        StringUtils.rightPad(cell.toString, colWidths(i))
      }
    }.addString(sb, "|", "|", "|\n")
  }
  
  sb.append(sep)
  
  // For Data that has more than "numRows" records
  if (hasMoreData) {
    val rowsString = if (numRows == 1) "row" else "rows"
    sb.append(s"only showing top $numRows $rowsString\n")
  }
    "\n"+sb.toString()
  }
  
  def getHiveForbiddenWords(
  ): List[String] = {log.info("cme@ getHiveForbiddenWords")
    // Non-reserved Keywords
    val nonReservedKeywords=List("ADD","ADMIN","AFTER","ANALYZE","ARCHIVE","ASC","BEFORE","BUCKET","BUCKETS","CASCADE","CHANGE","CLUSTER","CLUSTERED","CLUSTERSTATUS","COLLECTION","COLUMNS","COMMENT","COMPACT","COMPACTIONS","COMPUTE","CONCATENATE","CONTINUE","DATA","DATABASES","DATETIME","DAY","DBPROPERTIES","DEFERRED","DEFINED","DELIMITED","DEPENDENCY","DESC","DIRECTORIES","DIRECTORY","DISABLE","DISTRIBUTE","ELEM_TYPE","ENABLE","ESCAPED","EXCLUSIVE","EXPLAIN","EXPORT","FIELDS","FILE","FILEFORMAT","FIRST","FORMAT","FORMATTED","FUNCTIONS","HOLD_DDLTIME","HOUR","IDXPROPERTIES","IGNORE","INDEX","INDEXES","INPATH","INPUTDRIVER","INPUTFORMAT","ITEMS","JAR","KEYS","KEY_TYPE","LIMIT","LINES","LOAD","LOCATION","LOCK","LOCKS","LOGICAL","LONG","MAPJOIN","MATERIALIZED","MINUS","MINUTE","MONTH","MSCK","NOSCAN","NO_DROP","OFFLINE","OPTION","OUTPUTDRIVER","OUTPUTFORMAT","OVERWRITE","OWNER","PARTITIONED","PARTITIONS","PLUS","PRETTY","PRINCIPALS","PROTECTION","PURGE","READ","READONLY","REBUILD","RECORDREADER","RECORDWRITER","REGEXP","RELOAD","RENAME","REPAIR","REPLACE","RESTRICT","REWRITE","RLIKE","ROLE","ROLES","SCHEMA","SCHEMAS","SECOND","SEMI","SERDE","SERDEPROPERTIES","SERVER","SETS","SHARED","SHOW","SHOW_DATABASE","SKEWED","SORT","SORTED","SSL","STATISTICS","STORED","STREAMTABLE","STRING","STRUCT","TABLES","TBLPROPERTIES","TEMPORARY","TERMINATED","TINYINT","TOUCH","TRANSACTIONS","UNARCHIVE","UNDO","UNIONTYPE","UNLOCK","UNSET","UNSIGNED","URI","USE","UTC","UTCTIMESTAMP","VALUE_TYPE","VIEW","WHILE","YEAR")
    // Reserved Words
    val reservedWords=List("ALL","ALTER","AND","ARRAY","AS","AUTHORIZATION","BETWEEN","BIGINT","BINARY","BOOLEAN","BOTH","BY","CASE","CAST","CHAR","COLUMN","CONF","CREATE","CROSS","CUBE","CURRENT","CURRENT_DATE","CURRENT_TIMESTAMP","CURSOR","DATABASE","DATE","DECIMAL","DELETE","DESCRIBE","DISTINCT","DOUBLE","DROP","ELSE","END","EXCHANGE","EXISTS","EXTENDED","EXTERNAL","FALSE","FETCH","FLOAT","FOLLOWING","FOR","FROM","FULL","FUNCTION","GRANT","GROUP","GROUPING","HAVING","IF","IMPORT","IN","INNER","INSERT","INT","INTERSECT","INTERVAL","INTO","IS","JOIN","LATERAL","LEFT","LESS","LIKE","LOCAL","MACRO","MAP","MORE","NONE","NOT","NULL","OF","ON","OR","ORDER","OUT","OUTER","OVER","PARTIALSCAN","PARTITION","PERCENT","PRECEDING","PRESERVE","PROCEDURE","RANGE","READS","REDUCE","REGEXP","REVOKE","RIGHT","RLIKE","ROLLUP","ROW","ROWS","SELECT","SET","SMALLINT","TABLE","TABLESAMPLE","THEN","TIMESTAMP","TO","TRANSFORM","TRIGGER","TRUE","TRUNCATE","UNBOUNDED","UNION","UNIQUEJOIN","UPDATE","USER","USING","VALUES","VARCHAR","WHEN","WHERE","WINDOW","WITH")
    
    nonReservedKeywords++reservedWords
  }
  def getHiveReservedWords(
  ): List[String] = {log.info("cme@ getHiveForbiddenWords")
    // Reserved Words
    val reservedWords=List("ALL","ALTER","AND","ARRAY","AS","AUTHORIZATION","BETWEEN","BIGINT","BINARY","BOOLEAN","BOTH","BY","CASE","CAST","CHAR","COLUMN","CONF","CREATE","CROSS","CUBE","CURRENT","CURRENT_DATE","CURRENT_TIMESTAMP","CURSOR","DATABASE","DATE","DECIMAL","DELETE","DESCRIBE","DISTINCT","DOUBLE","DROP","ELSE","END","EXCHANGE","EXISTS","EXTENDED","EXTERNAL","FALSE","FETCH","FLOAT","FOLLOWING","FOR","FROM","FULL","FUNCTION","GRANT","GROUP","GROUPING","HAVING","IF","IMPORT","IN","INNER","INSERT","INT","INTERSECT","INTERVAL","INTO","IS","JOIN","LATERAL","LEFT","LESS","LIKE","LOCAL","MACRO","MAP","MORE","NONE","NOT","NULL","OF","ON","OR","ORDER","OUT","OUTER","OVER","PARTIALSCAN","PARTITION","PERCENT","PRECEDING","PRESERVE","PROCEDURE","RANGE","READS","REDUCE","REGEXP","REVOKE","RIGHT","RLIKE","ROLLUP","ROW","ROWS","SELECT","SET","SMALLINT","TABLE","TABLESAMPLE","THEN","TIMESTAMP","TO","TRANSFORM","TRIGGER","TRUE","TRUNCATE","UNBOUNDED","UNION","UNIQUEJOIN","UPDATE","USER","USING","VALUES","VARCHAR","WHEN","WHERE","WINDOW","WITH")
    reservedWords
  }  
  /*
   *  the charBuckets are 
   * 256 buckets for the 256 ascii chaacters (0 thru 255), 
   * #256 for all characters beyond 255, 
   * #257 is sum of all non-digit
   * #258 sum of all alphabeticals
   * #259 sum of all digits
   * #260 sum of all typeableNonAlphaNumeric
   * #261 sum of all non-typeableNonAlphaNumeric
   * #262 sum of all typeables indicating not-a-number           except leadingHyphen or decimalPoint   (numbers can have negative signs, and periods )
   * #263 sum of all typeables indicating not-a-dateOrTimestamp  except / non-leading-Hyphens  : space  decimalPoints T or  Z    (for dates and times)
   * #264 sum of all leading hyphenOrMinusSigns
   * #265 sum of all non-leading hypenOrMinusSigns
   * 
   */
  def logAsciiHeader(
    space2TheLeft : Int,
    maxSizeOfCounts: Int,
    includeColumns : Array[Boolean]
  ){
    val useMSOC=if(4>maxSizeOfCounts){1}else{maxSizeOfCounts-2}
    //log.info("maxSizeOfCounts="+maxSizeOfCounts+" useMSOC="+useMSOC)
    val sb = new StringBuilder()
    sb.append( {s"%${space2TheLeft}s "}.format(" "))
    for(ii<- 0 until 267)if((255<ii)||(ii>=includeColumns.length)||includeColumns(ii))sb.append(s"%${useMSOC}s%3d".format(" ",ii))
    log.info(sb)
    
    val ascii=getAsciiAsPrints3WideStrings
    sb.clear()
    sb.append( {s"%${space2TheLeft}s "}.format(" "))
    for((asc,ii)<-ascii.zipWithIndex){
      if((255<ii)||(ii>=includeColumns.length)||includeColumns(ii))sb.append(s"%${useMSOC}s%s".format(" ",asc))}
    log.info(sb)
  }
  def removePreambles(columnNames : ArrayBuffer[String]
  ): Boolean = {
    log.info("cme@ removePreambles")
    if(1<columnNames.length){
      //columnNames.foreach(f=>log.info("precheck on columnNames "+f))
      val lesser=if(columnNames(0).length<columnNames(1).length){columnNames(0).length} else {columnNames(1).length}
      /**/log.info("lesser="+lesser)
      var matchChars=0;
      while(  (lesser>matchChars)
            &&(columnNames(0).substring(0,1+matchChars)==columnNames(1).substring(0,1+matchChars))
           ){
          /**/log.info(f"ok with matchChars=$matchChars%3d ${columnNames(0).substring(0,1+matchChars)}%s")
        matchChars+=1
      }
      /**/log.info("compare of 1st and second columns gave matchChars="+matchChars+" "+columnNames(0).substring(0,matchChars))
      val matches=columnNames(0).substring(0,matchChars)
      if(  (0<matchChars)
         &&(matches(matchChars-1)=='.')
        ){
        var trueForAll=true
        for(ii<-2 until columnNames.length){
          trueForAll&=(if(matchChars<=columnNames(ii).length)columnNames(0).substring(0,matchChars)==columnNames(ii).substring(0,matchChars) else false)
        }
        log.info("trueForAll="+trueForAll)
        if(trueForAll){
          for((cn,ii) <- columnNames.zipWithIndex){
            columnNames(ii)=cn.substring(matchChars)
            log.info(f"$ii%3d ${columnNames(ii)}%s")
          }
        }
        return(trueForAll)
      } else {
        return false
      }
    }
    true
  }
    
  def getAsciiAsPrints3WideStrings(
  ): Array[String] = {
    //log.info("cme@ getAsciiAsPrints3WideStrings")
    val ascii = Array.fill(267)(" ")
    ascii( 0)="nul"
    ascii( 1)="SOH"
    ascii( 2)="STX"
    ascii( 3)="ETX"
    ascii( 4)="EOT"
    ascii( 5)="ENQ"
    ascii( 6)="ACK"
    ascii( 7)="BEL"
    ascii( 8)=" BS"
    ascii( 9)="tab"
    ascii(10)=" lf"
    ascii(11)=" VT"
    ascii(12)=" FF"
    ascii(13)=" cr"
    ascii(14)=" SO"
    ascii(15)=" SI"
    ascii(16)="DLE"
    ascii(17)="DC1"
    ascii(18)="DC2"
    ascii(19)="DC3"
    ascii(20)="DC4"
    ascii(21)="NAK"
    ascii(22)="SYN"
    ascii(23)="ETB"
    ascii(24)="CAN"
    ascii(25)=" EM"
    ascii(26)="SUB"
    ascii(27)="ESC"
    ascii(28)=" FS"
    ascii(29)=" GS"
    ascii(30)=" RS"
    ascii(31)=" US"
    ascii(32)=" sp"
    for(ii<-33 until 128)ascii(ii)="  "+ii.toChar.toString
    ascii(128)="  €"
    ascii(129)="   "
    ascii(130)="  ‚"
    ascii(131)="  ƒ"
    ascii(132)="  „"
    ascii(133)="  …"
    ascii(134)="  †"
    ascii(135)="  ‡"
    ascii(136)="  ˆ"
    ascii(137)="  ‰"
    ascii(138)="  Š"
    ascii(139)="  ‹"
    ascii(140)="  Œ"
    ascii(141)="   "
    ascii(142)="  Ž"
    ascii(143)="   "
    ascii(144)="   "
    ascii(145)="  ‘"
    ascii(146)="  ’"
    ascii(147)="  “"
    ascii(148)="  ”"
    ascii(149)="  •"
    ascii(150)="  –"
    ascii(151)="  —"
    ascii(152)="  ˜"
    ascii(153)="  ™"
    ascii(154)="  š"
    ascii(155)="  ›"
    ascii(156)="  œ"
    ascii(157)="   "
    ascii(158)="  ž"
    ascii(159)="  Ÿ"
    ascii(160)="  ¡"
    ascii(161)="  ¢"
    val seed =    "  €     ‚  ƒ  „  …  †  ‡  ˆ  ‰  Š  ‹  Œ     Ž        ‘  ’  “  ”  •  –  —  ˜  ™  š  ›  œ     ž  Ÿ  ¡  ¢  £  ¤  ¥  ¦  §  ¨  ©  ª  «  ¬  ­  ®  ¯  °  ±  ²  ³  ´  µ  ¶  ·  ¸  ¹  º  »  ¼  ½  ¾  ¿  À  Á  Â  Ã  Ä  Å  Æ  Ç  È  É  Ê  Ë  Ì  Í  Î  Ï  Ð  Ñ  Ò  Ó  Ô  Õ  Ö  ×  Ø  Ù  Ú  Û  Ü  Ý  Þ  ß  à  á  â  ã  ä  å  æ  ç  è  é  ê  ë  ì  í  î  ï  ð  ñ  ò  ó  ô  õ  ö  ÷  ø  ù  ú  û  ü  ý  þ  ÿ  ÝtooHi"
    for(ii<-128 until 256)ascii(ii)=seed.substring(3*(ii-128),3*(ii-127))
    ascii(256)="2Hi"
    ascii(257)="Ndg"
    ascii(258)="Let"
    ascii(259)="Dig"
    ascii(260)="typ"
    ascii(261)="NTP"
    ascii(262)="NAN"
    ascii(263)="NTS"
    ascii(264)="L- "
    ascii(265)="NL-"
    ascii(266)="LMZ"
    //for(ii<- 0 until 267)log.info(s"%3d %s %s %s %s".format(ii,ascii(ii),ii.toChar.isDigit,ii.toChar.isLetter,ii.toChar.isLetterOrDigit))
    ascii
  }
}
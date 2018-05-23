package myScalaUtils

import scala.collection.mutable.ArrayBuffer
import com.typesafe.config._
import java.io.File
import java.util.Properties
import scala.io.Source
import org.apache.log4j.{Level, Logger, LogManager, PropertyConfigurator}
import java.text.SimpleDateFormat;
import scala.collection.JavaConversions._
import java.util.Date;

object CustomizeProperties {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("CustomizeProperties")
  
  def seekDefaults (
     config :Config
    ,props : Properties
    ,noOneIsGoingToHaveMoreActionsThanThis : Int
    ,controls : Array[Seq[String]]
  ){
    log.info("cme@ seekDefaults")
    var base=""
    var ancillary0=""
    var ancillary1=""
    var ii=noOneIsGoingToHaveMoreActionsThanThis

      /* diagnostics wanted to see what was config on the way in */    
//    val ab = new ArrayBuffer[String]()
//    val iteratorPC = config.entrySet().iterator();
//    var jj=0
//    while(iteratorPC.hasNext()) {
//      val entry = iteratorPC.next()
//      val in=entry.getValue().render().trim()
//      val inl=in.toLowerCase()
//      ab+=String.format("App %-38s %s",entry.getKey(),(if(inl.contains("password")&& !inl.contains("passwordfid")){"XXXXXXXX"}else {in}))
//      jj+=1
//    }
//    log.info("jj="+jj+" incomming to CustomizeProperties config=")
//    ab.sortWith(_<_).foreach { x => log.info(x) }
    
    while (0 <= ii){
      //log.info(f"default inserting ii=$ii%02d")
      controls.foreach(control =>
        try {
          //log.info("control.length="+control.length+" control="+control.toString)
          base=f"$ii%02d_act"
          //log.info("sniffing for base="+base)
          val act=config.getString(base)
          //log.info("value of "+base+" = "+act)
          if( control(0) ==act) {
            //log.info("see an act, that act being "+control(0))
            try {
              ancillary0=f"$ii%02d_"+control(1)
              //log.info("peeking for "+ancillary0)
              config.getString(ancillary0)
            } catch {
            case ce:  ConfigException=>  
              //log.info("got no  "+ancillary0)
              var jj=ii
              while(0<=jj){
                ancillary1=f"$jj%02d_"+control(2)
                //log.info("Sniffing for first prior mutateable instance with ancillary1="+ancillary1) 
                try {
                  config.getString(ancillary1)
                  //log.info("a hit a palpable hit")
                  var value=control(3) match {
                    case "keep"    => config.getString(ancillary1)
                    case "append"  => config.getString(ancillary1)+control(4)
                    case "prepend" => control(4)+config.getString(ancillary1)
                    case "replace" => config.getString(ancillary1).replace(control(4),control(5))
                  }
                  if(5<control.length)value=control(5) match {
                    //case "keep"    => config.getString(ancillary1)
                    //case "append"  => config.getString(ancillary1)+control(4)
                    //case "prepend" => control(4)+config.getString(ancillary1)
                    case "replace" => value.replace(control(6),control(7))
                  }
                  //log.info("setting in place key="+ancillary0+"   value="+value)
                  props.setProperty(ancillary0,value)
                  jj=0
                } catch {
                  case ce:  ConfigException=>  
                  /*  log.info(f"tripped ConfigException on $ii%2d_act") */
                }
                jj-=1
              }
            }
          }
        } catch {
         case ce:  ConfigException=>  
         //log.info(f"outermost catch ii="+ii)
        }
      )  
      ii-=1
    }
  }
  
  def customizeProperties(
      config  : Config,
      props : Properties
    ) {
    log.info("cme@ customizeProperties")
   
    props.setProperty("allowCharDataTypes"                     ,"false") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("allowIntegerDataTypeSmall"              ,"false")
    props.setProperty("allowIntegerDataTypeTiny"               ,"false")
    props.setProperty("aws_access_key_id"                       ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("aws_secret_access_key"                   ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("bailOnAnalysisOfMostFrequent"           ,"false")
    props.setProperty("current_date_override"                   ,"")
    props.setProperty("dateAndTimestampFormatNumRows2Sample"   ,"100")
    props.setProperty("delimiterSniffRowCount"                 ,"100")
    props.setProperty("numberOfMostFrequentItems"              ,"512") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("outputEveryCellFromSource2TargetCsvIntoLog","false") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("removeColumnNamePreambles"              ,"false")
    props.setProperty("S3SourceBucketName"                      ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("S3SourcePrefixOmit"                      ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("S3SourceRegion"                          ,"us-east-1") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("S3SourceRootPrefix"                      ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("S3TargetBucketName"                      ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("S3TargetPrefixPrefix"                          ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("S3TargetPrefixPrefix"                          ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("S3TargetRegion"                                ,"us-east-1") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("setIntegerDataTypeOnColumnMinMaxUsing2Xbuffer" ,"false")
    props.setProperty("setVarCharLengthsWith2XBuffer"                 ,"false") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("srcDirPart2BOmitted"                           ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("useTimeStamps4Dates"                           ,"false") /* setting default false because parquet supports dates, Set to True for output to Cloudera and HDB parquet's which have timestamps but not dates */
    props.setProperty("winutilsDotExeLocation"                       ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("source2TargetCsvFid"                          ,"./logs/z_source2Target.csv") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("csvFid"                                        ,"")
    props.setProperty("inputFid"                                      ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("outputLocation"                                ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("outputModification"                                ,"") 
    props.setProperty("hiveDB"                                        ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("visDownSpeedUp"                   ,"false") /* setting default so as to avoid exception for missing configuration setting *///    props.setProperty(""                   ,"") /* setting default so as to avoid exception for missing configuration setting */
    props.setProperty("applySas7bdatDateHack"                   ,"false") /* setting default so as to avoid exception for missing configuration setting */
//    props.setProperty(""                   ,"") /* setting default so as to avoid exception for missing configuration setting */
//    props.setProperty(""                   ,"") /* setting default so as to avoid exception for missing configuration setting */
//    props.setProperty(""                   ,"") /* setting default so as to avoid exception for missing configuration setting */
//    props.setProperty(""                   ,"") /* setting default so as to avoid exception for missing configuration setting */
//    props.setProperty(""                   ,"") /* setting default so as to avoid exception for missing configuration setting */
//    props.setProperty(""                   ,"") /* setting default so as to avoid exception for missing configuration setting */
      
    var noOneIsGoingToHaveMoreActionsThanThis=100
//    noOneIsGoingToHaveMoreActionsThanThis=7
    var ii=noOneIsGoingToHaveMoreActionsThanThis
    while (0 <= ii){
      try {
        //log.info(f"gofor $ii%2d_act")
        config.getString(f"$ii%02d_act")
        val numString=f"${ii}%d"
        props.setProperty("maxActNum",numString )
        //val msg="set maxActNum="+numString
        //println(msg)
        //log.info(msg)
        ii=0
      } catch {
       case ce:  ConfigException=>  
         //log.info(f"tripped ConfigException on $ii%2d_act")
      }
      ii-=1
    }
    val controls=new ArrayBuffer[Seq[String]]()
    /*              action                          value                            default based on     mutation0  graft     mutation1  replace   with */
    /*                0                                1                                2                   3         4          5        6         7   */
    controls+=Seq("convert2Parquet"         ,"targetFileSystemType"              ,"sourceFileSystemType","keep"                                        )
    controls+=Seq("convert2Parquet"         ,"targetRootLocation"                ,"sourceRootLocation"  ,"append","/parquet","replace","landing","raw" )
    controls+=Seq("DDL_discovery"           ,"dataDiscoveredDotTxtFileSystemType","sourceFileSystemType","keep"                                        )
    controls+=Seq("DDL_discovery"           ,"dataDiscoveredDotTxtLocation"      ,"sourceRootLocation"  ,"append","/ddl"                               )
    controls+=Seq("outputHiveCreateTableHQL","targetRootLocation"                ,"sourceRootLocation"  ,"append","/hql"                               )
    seekDefaults(config,props,noOneIsGoingToHaveMoreActionsThanThis,controls.toArray)
  }
}
package myScalaUtils

import org.apache.log4j.{Level, LogManager}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import com.typesafe.config._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,RawLocalFileSystem,LocalFileSystem,Path,PathFilter,FileStatus,LocatedFileStatus}
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.log4j.Logger

//import java.io.File
import java.io.FileReader
import java.io.BufferedOutputStream
import java.io.FileOutputStream

import com.opencsv.CSVReader;
import scala.collection.JavaConversions._
import scala.util.Random
import scala.collection.immutable.ListMap
import myScalaUtils.MiscellaneousUtils._

object FileIO {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("FileIO")

  def fileO(
     fs:FileSystem
    ,lfs:LocalFileSystem
    ,useLocalFs : Boolean = true
    ,fid:String
    ,body:StringBuilder
  ): Unit = {
    
    log.info("inside FileO fid="+fid+" and useLocalFs="+(if(useLocalFs)"true"else"false")+" sb.length="+body.length)
//    var fid=""
//    if(inFid.startsWith("./")){
//      fid=(if(useLocalFs)lfs else fs).getWorkingDirectory+inFid.substring(1)
//    } else {
//      fid=inFid
//    }
//    log.info("fid="+fid)
//   log.info("fs.getWorkingDirectory="+fs.getWorkingDirectory)
//    log.info("path allready exists? "+fs.exists(new Path(fid)))
//    log.info("log path allready exists? "+fs.exists(new Path("/home/hadoop/tpmoyer/framework/logs/KIF_V2.02.log")))
    val p=new Path(fid)
    val fsPath=if(useLocalFs)lfs.mkdirs(p.getParent) else fs.mkdirs(p.getParent)
    log.info("\npath = "+p+"\np.name="+p.getName+"\np.parent="+p.getParent+"      ====> mkdirs reports "+fsPath+ "\nqualified="+p.makeQualified(fs))
    val fsdos=if(useLocalFs)lfs.create(new Path(fid)) else fs.create(new Path(fid))
    //log.info("create ok")
    val os = new BufferedOutputStream(fsdos)
    os.write(body.toString().toCharArray().map(_.toByte))
    os.close()

  }
  
  def getArrayOfFileStatuses(
     fs :FileSystem
    ,dir : String
    ,recursive : Boolean
  ):Array[FileStatus]={
    val lof=getFSListOfFiles(fs,dir,recursive)
    log.info((if(recursive)""else"non")+"recursive getFSListOfFiles length="+lof.length)
    for((f,ii)<-lof.zipWithIndex)log.info(f"$ii%3d ${f.getPath}%s ")
    lof
  }
//  def rename(oldName: String, newName: String) = {
//   import util.Try
//   Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
//  }
  
//	def deletePath(fs: FileSystem, fid : String,recursive : Boolean): Unit = {
//	  val path=new Path(fid)
//	  log.info("pre  "+(if(fs.exists(path))"true "else "false")+" deleteRecursively on "+fid)
//	  val rc=fs.delete(path,recursive)
//	  log.info("post "+(if(fs.exists(path))"true "else "false")+" with rc reporting "+rc)
//	}
  	
//	def deleteRecursively(file: File): Unit = {
//	  log.info("pre  "+(if(file.exists)"true "else "false")+" deleteRecursively on "+file.getCanonicalFile)
//		if (file.isDirectory) file.listFiles.foreach(deleteRecursively)	
//		if (file.exists && !file.delete)throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
//	  log.info("post "+(if(file.exists)"true "else "false"))
//	}
  

  

  
//  def fileO(fid:String,body:StringBuilder): Unit = {
//    import java.io._
//    //val fid ="./y_countRawPscSurveys.hql"
//    try {
//      log.info("write attempt to "+fid)
//      
//      val path=(if(fid.contains("\\"))fid.substring(0,fid.lastIndexOf("\\")) else if(fid.contains("/"))fid.substring(0,fid.lastIndexOf("/")) else "")
//      if(0<path.length){
//         val directory = new File(String.valueOf(path))
//         if (! directory.exists())directory.mkdirs()
//      }
//      val bw = new BufferedWriter(new FileWriter(new File(fid)))
//      bw.write(body.toString())
//      bw.close()
//    } catch {
//      case e: Exception => 
//        log.info("e Exception. fileIO write non-specific-handled error on "+fid+"\n"+e.getMessage+"\n"+e.getStackTrace) 
//        e.printStackTrace() 
//    }
//  }

/* This follows the Scala Cookbook by Alvin Alexander
 */


//  /*
//  * Generates big test file.
//  */
//  def generateBigFile(fid:String,megabytes: Int) = {
//    val kilobytes = megabytes.toLong * 1024
//    val buffer = new Array[Byte](1024)
//    val file = new File(fid)
//    val out = new BufferedOutputStream(new FileOutputStream(file))
//    try {
//      var idx = 0L
//      while (idx < kilobytes) {
//        Random.nextBytes(buffer)
//        out.write(buffer)
//        idx += 1
//      }
//      file
//    } finally {
//      out.close()
//    }
//  }
//  def getRecursiveListOfFiles(dir: File): Array[File] = {
//    val these = dir.listFiles
//    if(null==these){
//      log.error("the recursive list of files from "+dir.getName+" is null.\nMost likely this is a non-existant directory")
//    } else {
//      these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
//    }  
//    these
//  }
//  def getNonRecursiveListOfFiles(dir: File): Array[File] = {
//    //log.info("cme@ getNonRecursiveListOfFiles")
//    dir.listFiles
//  }
  
  /* the FS and the return type being FileStatus are part of shifting all
   * file IO over to hadoop.fs
   * This Method returns only things which are files, no directories. 
   */
  def getFSListOfFiles(
    fs      : FileSystem   
   ,dirName : String
   ,recurse : Boolean
   ): Array[FileStatus] = {
    val alist=ArrayBuffer[FileStatus]()
    val fileStatusListIterator = fs.listFiles(new Path(dirName), recurse)
    while(fileStatusListIterator.hasNext())alist+=fileStatusListIterator.next()
    alist.toArray
  }
//  /* the FS and the return type being FileStatus are part of shifting all
//   * file IO over to hadoop.fs
//   * This Method returns files and directories 
//   */
//  def getFSNonRecursiveListOfFilesAndDirectories(
//    fs      : FileSystem   
//   ,dirName : String
//   ): Array[FileStatus] = {
//     val lStatuss = fs.listStatus(new Path(dirName),new PathFilter {
//       /* instead of the trivial "true" the accept method can be any filtering method returning a boolean */ 
//        override def accept(path: Path) = true   /* this accepts ALL fileStatus's */ 
//     })
//     lStatuss
//   }  


  
//  def fullFileParamDumpOnRecursiveListOfFiles(dir: File){
//    log.info("directory="+dir.getName());
//    val alist= FileIO.getRecursiveListOfFiles(dir)
//    for((a,ii)<-alist.zipWithIndex){
//      log.info(f"file $ii%4d ${a.getName}%s")
//      log.info("     idLocation()      ="+a.isDirectory())
//      log.info("     isFile()           ="+a.isFile())
//      log.info("     canRead()          ="+a.canRead())
//      log.info("     canWrite()         ="+a.canWrite())
//      log.info("     canExecute()       ="+a.canExecute())
//      log.info("     exists()           ="+a.exists())
//      log.info("     isAbsolute()       ="+a.isAbsolute())
//      log.info("     isHidden()         ="+a.isHidden())
//      log.info("     lastModified()     ="+a.lastModified())
//      log.info("     toURI()            ="+a.toURI())
//      log.info("     getAbsoluteFile()  ="+a.getAbsoluteFile())
//      log.info("     getAbsolutePath()  ="+a.getAbsolutePath())
//      log.info("     getCanonicalFile() ="+a.getCanonicalFile())
//      log.info("     getCanonicalPath() ="+a.getCanonicalPath())
//      log.info("     getFreeSpace()     ="+a.getFreeSpace())
//      log.info("     getName()          ="+a.getName())
//      log.info("     getParent()        ="+a.getParent())
//      log.info("     getParentFile()    ="+a.getParentFile())
//      log.info("     getPath()          ="+a.getPath())
//      log.info("     getTotalSpace()    ="+a.getTotalSpace())
//      log.info("     getUsableSpace()   ="+a.getUsableSpace())
//      log.info("     length()           ="+a.length())
//    }
//  }
//  def curatedFileParamDump2LogInfoOnListOfFiles(
//    dir: File,
//    recursive:Boolean,
//    includeDirectories:Boolean
//  ){
//    log.info("directory="+dir.getName()+" recursive="+((if(recursive){"true"}else{"false"}))+" includeDirectories(false means only files get logged)="+(if(includeDirectories){"true"}else{"false"})+"\n");
//    val os=System.getProperty("os.name")
//    import java.text.SimpleDateFormat;
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    
//    val blist=Option(if(recursive){FileIO.getRecursiveListOfFiles(dir)}else {FileIO.getNonRecursiveListOfFiles(dir)})
//    val alist=blist.getOrElse(new Array[File](0))
//    if(alist.isEmpty){
//      log.info("directory has no files")
//    } else {  
//      var maxLengthLength=0
//      var maxLengthCanonicalParent=0
//      for(a<-alist){
//        if(includeDirectories||a.isFile){
//          if(maxLengthLength < a.length.toString.length){
//            maxLengthLength = a.length.toString.length
//          }
//          if(maxLengthCanonicalParent < a.getCanonicalPath.length-1-a.getName.length){
//            maxLengthCanonicalParent = a.getCanonicalPath.length-1-a.getName.length
//          }
//        }
//      }
//      if(os.startsWith("Win"))maxLengthCanonicalParent-=2
//      //log.info("maxlengths are "+maxLengthLength+" "+maxLengthCanonicalParent)
//      var indexCounter=0
//      for((a,ii)<-alist.zipWithIndex){
//        if(includeDirectories||a.isFile){
//          val s={s"%4d %s%s%s %5s %${maxLengthLength}d %s %2s %-${maxLengthCanonicalParent}s %s"}.format(
//            indexCounter, 
//            if(a.canRead){"r"}else{"-"},
//            if(a.canWrite){"w"}else{"-"}, 
//            if(a.canExecute){"x"}else{"-"}, 
//            a.isFile,
//            a.length,
//            sdf.format(a.lastModified()),
//            if(os.startsWith("Win")){a.getCanonicalPath.substring(0,2)}else{"na"},
//            a.getCanonicalPath.substring(
//              if(os.startsWith("Win")){2}else{0},
//              a.getCanonicalPath.length-a.getName.length-1
//            ),
//            a.getName
//          )
//          log.info(s)
//          indexCounter+=1
//        }
//      }
//    }
//  }
  /* file Input    The ArrayBuffer would get overheadish on non-small files */
  def fileI(fid:String,lines:ArrayBuffer[String]): Unit = { 
    import scala.io.Source
    import java.io.{FileNotFoundException, IOException}
    try {
      for (line <- Source.fromFile(fid).getLines) {
        lines+=line
      }
    } catch {
      case e: FileNotFoundException => println("\nfileI Couldn't find file "+fid+"\n"+e.getStackTraceString)
      case e: IOException => println("\nfileI Got an IOException!\n"+e.getStackTraceString)
    } 
  }
//  def readOpenCsvFile(fid:String, rows:ArrayBuffer[Array[String]],hasHeader:Boolean, header:ArrayBuffer[String]): Unit = {
//    val reader = new CSVReader(new FileReader(fid))
//    reader.readAll().foreach { row => rows+=row }
//    if(hasHeader){
//      rows(0).foreach { col => header+=col}
//      rows.remove(0)
//    }
//    /* It has occured that a manually edited .csv file had a fully empty last row.  
//     * Purge this if it occurs
//     */
//    var purge=true
//    rows(rows.length-1).foreach { x => if(0<x.length())purge=false }
//    if(purge){
//      log.info("last line of "+fid+" is completely blank.  Am purging")
//      rows.remove(rows.length-1)
//    }
//  }
  def readOpenCsvFile(fid:String, hasHeader:Boolean, header:ArrayBuffer[String]): ArrayBuffer[Array[String]]= {
    val reader = new CSVReader(new FileReader(fid))
    val rows=ArrayBuffer[Array[String]]()
    reader.readAll().foreach { row => rows+=row }
    reader.close
    if(hasHeader){
      rows(0).foreach { col => header+=col}
      rows.remove(0)
    }
    /* It has occured that a manually edited .csv file had a fully empty last row.  
     * Purge this if it occurs
     */
    var purge=true
    rows(rows.length-1).foreach { x => if(0<x.length())purge=false }
    if(purge){
      log.info("last line of "+fid+" is completely blank.  Am purging")
      rows.remove(rows.length-1)
    }
    rows
  }
  import java.io.File
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
  /* This follows "How to Process a CSV File" from the Scala Cookbook by Alvin Alexander */
  /* as such it is fine for simple csv files, but as soon as anything like an imbedded comma occurs
   * this parses the lines badly
   */
  //  def   readCSVFile(fid:String, rows:ArrayBuffer[Array[String]],hasHeader:Boolean, header:ArrayBuffer[String]): Unit = {
  //    if(hasHeader){
  //      using(Source.fromFile(fid)) { source =>
  //        for (line <- source.getLines.take(1)) {
  //          line.split(",",-1).map(_.trim).foreach{ x => header+=x }
  //        }
  //      }
  //    }
  //    using(Source.fromFile(fid)) { source =>
  //      for (line <- source.getLines.drop(if(hasHeader)1 else 0)) {
  //        rows += line.split(",",-1).map(_.trim)
  //      }
  //    }
  //  }
  
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }
}
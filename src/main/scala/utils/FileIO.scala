package utils

import common.FileSystemManager._
import org.apache.log4j.{Level, LogManager}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import com.typesafe.config._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,RawLocalFileSystem,LocalFileSystem,Path,PathFilter,FileStatus,LocatedFileStatus,FSDataInputStream,FSDataOutputStream}
//import org.apache.hadoop.fs._
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.log4j.Logger

import java.io.File
import java.io.FileReader
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.io.IOException

import com.opencsv.CSVReader;
import scala.collection.JavaConversions._
import scala.util.Random
import scala.collection.immutable.ListMap
import utils.MiscellaneousUtils._
import common.ExceptionHandler
import java.net.URI
import com.typesafe.config.Config


//import awscala._, s3._
//import com.amazonaws.AmazonClientException
//import com.amazonaws.AmazonServiceException
//import com.amazonaws.auth.profile.ProfileCredentialsProvider
//import com.amazonaws.auth.PropertiesCredentials
//import com.amazonaws.auth.BasicAWSCredentials
//import com.amazonaws.auth.AWSStaticCredentialsProvider
//import com.amazonaws.regions.Regions._
//import com.amazonaws.services.s3._
//import com.amazonaws.services.s3.AmazonS3
//import com.amazonaws.services.s3.AmazonS3Client._
//
////import com.amazonaws.services.s3.model._ /* NOOoooo this conflict with the s3._, so add methods and types incrementally */ 
//import com.amazonaws.services.s3.model.{
//	AbortMultipartUploadRequest,
//	CompleteMultipartUploadRequest,
//	CopyObjectRequest,
//	CopyPartRequest,
//	CopyPartResult,
//	InitiateMultipartUploadRequest,
//	PartETag,
//	PutObjectRequest,
//	UploadPartRequest
//}
//import com.amazonaws.services.s3.transfer.{TransferManager,ObjectMetadataProvider}


object FileIO {
	@transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.substring(0,this.getClass.getName.length-1))
	private val ReadFileErrorCode = 304
	private val WriteBytesErrorCode = 303
	
	def fileI(
		 config: Config
		,fid: String
		,useLocalFs : Boolean =false
	): ArrayBuffer[String] = {
		val fs = fileSystem(config,useLocalFs)
		log.info("about to attempt read on fs.schema="+fs.getScheme+" fileIDentifier="+fid)
		val fsdis: FSDataInputStream  = ExceptionHandler.callOrExit(ReadFileErrorCode, log, Some(s"Failed opening FSDataInputStream: $fid"))(fs.open(new Path(fid)))
		val lines= new ArrayBuffer[String]()
		ExceptionHandler.usingOrExit(ReadFileErrorCode, log, new BufferedReader(new InputStreamReader(fsdis))){ case br =>
			var line = br.readLine
			while(line != null){
				lines += line
				line = br.readLine
			}
		}
		
		/* Exit if file is empty */
		if(lines.length <= 0)ExceptionHandler.logStackTraceAndExit(log, new IOException(s"$fid is empty"), ReadFileErrorCode)
		
		lines
	}	
	def fileO(
		 config: Config
		,fid        : String
		,body       : String
		,useLocalFs : Boolean = false
	): Unit = {
		val fs = fileSystem(config,useLocalFs)
		log.info("about to attempt writeBytes on fs.schema="+fs.getScheme+" fileIDentifier="+fid)
		val fsdos: FSDataOutputStream = ExceptionHandler.callOrExit(WriteBytesErrorCode, log, Some(s"Failed creating path: $fid"))(fs.create(new Path(fid)))
		val os = new BufferedOutputStream(fsdos)
		os.write(body.toCharArray.map(_.toByte))
		os.close()
	}
	
	/* this will see only files, is blind to directories */
	def getArrayOfFileStatuses(		
		 config : Config
		,dir         : String
		,recursive   : Boolean
		,useLocalFs  : Boolean = false
		,suppressLogListing : Boolean = false
	):Array[FileStatus]={
		if(suppressLogListing){
			fileSystem(config,useLocalFs).listStatus(new Path(dir))
		} else {	
			val lof=fileSystem(config,useLocalFs).listStatus(new Path(dir))
			log.info((if(recursive)""else"non")+"recursive getFSListOfFiles length="+lof.length)
			for((f,ii)<-lof.zipWithIndex)log.info(f"$ii%3d ${f.getPath}%s ")
			lof
		}
	}
	
	def rename(oldFid: String, newFid: String) = {
		import util.Try
		Try(new File(oldFid).renameTo(new File(newFid))).getOrElse(false)
	}
	
	def deletePath(
		 config : Config
		,dirName    : String
		,recursive  : Boolean
		,useLocalFs : Boolean = false
	): Unit = {
		val fs=fileSystem(config,useLocalFs)
		val path=new Path(dirName)
		log.info("pre  "+(if(fs.exists(path))"true "else "false")+" deleteRecursively on "+dirName)
		val rc=fs.delete(path,recursive)
		log.info("post "+(if(fs.exists(path))"true "else "false")+" with rc reporting "+rc)
	}

	/*
	* Generates big test file.
	*/
	def generateBigFile(fid:String,megabytes: Int) = {
		val kilobytes = megabytes.toLong * 1024
		val buffer = new Array[Byte](1024)
		val file = new File(fid)
		val out = new BufferedOutputStream(new FileOutputStream(file))
		try {
			var idx = 0L
			while (idx < kilobytes) {
				Random.nextBytes(buffer)
				out.write(buffer)
				idx += 1
			}
			file
		} finally {
			out.close()
		}
	}
	
	/* This Method returns only files, it is blind to directories */
	def getListOfFiles(
		 config: Config
		,dirName : String
		,recurse : Boolean
		,useLocalFs : Boolean = false
	): Array[FileStatus] = {
		val alist=ArrayBuffer[FileStatus]()
		val fileStatusListIterator = fileSystem(config,useLocalFs).listFiles(new Path(dirName), recurse)
		while(fileStatusListIterator.hasNext())alist+=fileStatusListIterator.next()
		alist.toArray
	}
	
	/* this method works only with the localFileSystem */
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
	
	
	
	
//TODO  finished development of an S3 filesystem peeker 
//	def getS3ArrayOfListStatus(
//					 config: Config
//		,anInputJavaFileDirectory: File  /* this File contains the filesystem information */
//		,recurse : Boolean = false
//			){
//		
////	 val uri = new URI("hdfs://ip-172-31-29-85.ec2.internal:8020")    
////  val fs = FileSystem.get(uri,new Configuration())    
////  val filePath = new Path("/dev/omop/")    
////  val status = fs.listStatus(filePath)    
////  status.map(sts => sts.getPath).foreach(println)
//  
//
//		import com.amazonaws.{ regions => awsregions }
//		val aRegion=awsregions.Region.getRegion(awsregions.Regions.fromName(config.getString("S3TargetRegion")))
//		implicit val s3 =if(config.getString("aws_access_key_id").startsWith("###########")){ 
//			S3().at(aRegion)
//		} else {	
//			S3(BasicCredentialsProvider(config.getString("aws_access_key_id"),config.getString("aws_secret_access_key"))).at(aRegion)
//		}	
//	
//			val uri = new URI("s3n://takdllandingdev")    
//		  val fs = FileSystem.get(uri,new Configuration())    
//		  val filePath = new Path("/dev/omop/")    
//		  val status = fs.listStatus(filePath)    
//		  status.map(sts => sts.getPath).foreach(println)
//			
//		  lnp("")
//		  val ofs=fileSystem(config)
//		  val ostatus = ofs.listStatus(filePath)    
//		  ostatus.map(sts => sts.getPath).foreach(println)
//	}
	
	
	
	
	
//TODO  update this set of persnikity get all the arcane information available on a local filesystem, generified to fileSystemManager
//	
//	def getArrayOfJavaFilesUnderAnInputJavaFileDirectory(
//		 config: Config
//		,anInputJavaFileDirectory: File  /* this File contains the filesystem information */
//		,recurse : Boolean
//	): Array[File] = {
//		val these = anInputJavaFileDirectory.listFiles
//		//for((t,ii)<-these.zipWithIndex)log.info(f"pre  $ii%4d ${t.getAbsoluteFile}%s")
//		if(null==these){
//			log.error("the recursive list of files from "+anInputJavaFileDirectory.getName+" is null.\nMost likely this is a non-existant directory")
//		} else if (recurse){
//			these ++ these.filter(_.isDirectory).flatMap(reentrantInputJavaFileDirectory)
//		}  
//		//for((t,ii)<-these.zipWithIndex)log.info(f"post $ii%4d ${t.getAbsoluteFile}%s")
//		these
//	}
//	
//	/* not expected to be called directly, only as a re-entrant from getArrayOfJavaFilesUnderAnInputJavaFileDirectory */
//	def reentrantInputJavaFileDirectory(anInputJavaFileDirectory: File): Array[File] = {
//		val these = anInputJavaFileDirectory.listFiles
//		if(null==these){
//			log.error("the recursive list of files from "+anInputJavaFileDirectory.getName+" is null.\nMost likely this is a non-existant directory")
//		} else {
//			these ++ these.filter(_.isDirectory).flatMap(reentrantInputJavaFileDirectory)
//		}  
//		these
//	}
//	
//	
//	 def getNonRecursiveListOfFiles(dir: File): Array[File] = {
//		//log.info("cme@ getNonRecursiveListOfFiles")
//		dir.listFiles
//	}
//	
//	def fullFileParamDumpOnRecursiveListOfFiles(
//		 config: Config
//		//,dirName : String
//		,dir :File
//		,recurse : Boolean
//	){
//		//log.info("directory="+dirName);
//		val alist=getArrayOfJavaFilesUnderAnInputJavaFileDirectory(config,dir,recurse) 
//		//getListOfFiles(config,dirName,recurse,useLocalFs)
//		for((a,ii)<-alist.zipWithIndex){
//			log.info(f"file $ii%4d ${a.getName}%s")
//			log.info("     idLocation()      ="+a.isDirectory())
//			log.info("     isFile()           ="+a.isFile())
//			log.info("     canRead()          ="+a.canRead())
//			log.info("     canWrite()         ="+a.canWrite())
//			log.info("     canExecute()       ="+a.canExecute())
//			log.info("     exists()           ="+a.exists())
//			log.info("     isAbsolute()       ="+a.isAbsolute())
//			log.info("     isHidden()         ="+a.isHidden())
//			log.info("     lastModified()     ="+a.lastModified())
//			log.info("     toURI()            ="+a.toURI())
//			log.info("     getAbsoluteFile()  ="+a.getAbsoluteFile())
//			log.info("     getAbsolutePath()  ="+a.getAbsolutePath())
//			log.info("     getCanonicalFile() ="+a.getCanonicalFile())
//			log.info("     getCanonicalPath() ="+a.getCanonicalPath())
//			log.info("     getFreeSpace()     ="+a.getFreeSpace())
//			log.info("     getName()          ="+a.getName())
//			log.info("     getParent()        ="+a.getParent())
//			log.info("     getParentFile()    ="+a.getParentFile())
//			log.info("     getPath()          ="+a.getPath())
//			log.info("     getTotalSpace()    ="+a.getTotalSpace())
//			log.info("     getUsableSpace()   ="+a.getUsableSpace())
//			log.info("     length()           ="+a.length())
//		}
//	}
//	def curatedFileParamDump2LogInfoOnListOfFiles(
//		config: Config	
//		,dir: File
//		,recurse:Boolean
//		,includeDirectories:Boolean
//	){
//		log.info("directory="+dir.getName()+" recurse="+((if(recurse){"true"}else{"false"}))+" includeDirectories(false means only files get logged)="+(if(includeDirectories){"true"}else{"false"})+"\n");
//		val os=System.getProperty("os.name")
//		import java.text.SimpleDateFormat;
//		val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//		
//		val blist=Option(if(recurse){FileIO.getArrayOfJavaFilesUnderAnInputJavaFileDirectory(config,dir,recurse)}else {FileIO.getNonRecursiveListOfFiles(dir)})
//		val alist=blist.getOrElse(new Array[File](0))
//		if(alist.isEmpty){
//			log.info("directory has no files")
//		} else {  
//			var maxLengthLength=0
//			var maxLengthCanonicalParent=0
//			for(a<-alist){
//				if(includeDirectories||a.isFile){
//					if(maxLengthLength < a.length.toString.length){
//						maxLengthLength = a.length.toString.length
//					}
//					if(maxLengthCanonicalParent < a.getCanonicalPath.length-1-a.getName.length){
//						maxLengthCanonicalParent = a.getCanonicalPath.length-1-a.getName.length
//					}
//				}
//			}
//			if(os.startsWith("Win"))maxLengthCanonicalParent-=2
//			//log.info("maxlengths are "+maxLengthLength+" "+maxLengthCanonicalParent)
//			var indexCounter=0
//			for((a,ii)<-alist.zipWithIndex){
//				if(includeDirectories||a.isFile){
//					val s={s"%4d %s%s%s %5s %${maxLengthLength}d %s %2s %-${maxLengthCanonicalParent}s %s"}.format(
//						indexCounter, 
//						if(a.canRead){"r"}else{"-"},
//						if(a.canWrite){"w"}else{"-"}, 
//						if(a.canExecute){"x"}else{"-"}, 
//						a.isFile,
//						a.length,
//						sdf.format(a.lastModified()),
//						if(os.startsWith("Win")){a.getCanonicalPath.substring(0,2)}else{"na"},
//						a.getCanonicalPath.substring(
//							if(os.startsWith("Win")){2}else{0},
//							a.getCanonicalPath.length-a.getName.length-1
//						),
//						a.getName
//					)
//					log.info(s)
//					indexCounter+=1
//				}
//			}
//		}
//	}
	
	
}
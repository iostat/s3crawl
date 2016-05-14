package io.stat.s3crawl

import java.io.{File, FileOutputStream}

import akka.actor.{Actor, ActorRef, Props}
import com.amazonaws.services.s3.AmazonS3
import io.stat.s3crawl.Messages.{Begin, FileComplete}
import io.stat.util.Logging

import scala.annotation.tailrec

/**
  * Crawls a directory and looks for files/other directories that need to be crawled
  *
  * @author Ilya Ostrovskiy (https://github.com/iostat/)
  */
class FileCrawlerActor(s3Client: AmazonS3, path: String, dispatcher: ActorRef, settings: CommandLineFlags)
  extends Actor with Logging {
  @tailrec final def downloadFile(): Unit = {
    val destFile = new File(settings.destination + "/" + path)
    val destParent = destFile.getParentFile
    if(destParent != null) {
      destParent.mkdirs()
    }

    if(destFile.exists()) {
      destFile.delete()
    }

    var writeFinished = false

    try {
      val listing = s3Client.getObject(settings.bucket, path)
      val inStream = listing.getObjectContent
      val outStream = new FileOutputStream(destFile)

      var bytesRead: Int = 0
      val buffer: Array[Byte] = new Array(8192)

      while(bytesRead != -1){
        outStream.write(buffer, 0, bytesRead)
        bytesRead = inStream.read(buffer)
      }

      inStream.close()

      outStream.flush()
      outStream.close()

      writeFinished = true

      dispatcher ! FileComplete(path)
    } catch {
      case x: Any =>
        logWarn(s"Got an exception when writing files $x")
    }

    if(!writeFinished) {
      logInfo(s"Retrying write of file $path")
      downloadFile()
    }
  }

  override def receive: Receive = {
    case Begin => downloadFile()
    case _ => logInfo("lol")
  }
}

object FileCrawlerActor{
  def props(s3Client: AmazonS3, path: String, dispatcher: ActorRef, settings: CommandLineFlags): Props =
    Props(new FileCrawlerActor(s3Client, path, dispatcher, settings))
}


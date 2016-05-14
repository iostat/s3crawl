package io.stat.s3crawl


import akka.actor.{Actor, ActorRef, Props}
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, ObjectListing, S3ObjectSummary}
import io.stat.util.Logging

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Crawls a directory and looks for files/other directories that need to be crawled
  *
  * @author Ilya Ostrovskiy (https://github.com/iostat/) 
  */
class DirectoryCrawlerActor(
    s3Client: AmazonS3,
    directory: String,
    recurse: Boolean,
    dispatcher: ActorRef,
    settings: CommandLineFlags) extends Actor with Logging {

  implicit val timeout: Timeout = new Timeout(2 days) // lol?

  @tailrec final def throttleConsumption(): Boolean = {
    Try(Await.result(dispatcher ? Messages.BackPressureStatus, Duration.Inf)) match {
      case Success(Messages.NoPressure)     => true
      case Success(Messages.PressureExists) =>
        Thread.sleep(1000)
        throttleConsumption()
      case Success(x: Any) =>
        logError(s"Got an unexpected result $x. Suiciding.")
        false
      case Failure(e: Throwable) =>
        logError(s"Could not query back pressure status from the dispatcher. Suiciding: $e")
        false
    }
  }

  @tailrec final def consumeListingResult(listing: ListObjectsV2Result): Unit = {
    for(dir <- listing.getCommonPrefixes) {
      if(recurse) {
        dispatcher ! Messages.Directory(dir)
      } else {
        dispatcher ! Messages.PhantomDirectory
      }
    }

    val summaries = listing.getObjectSummaries.toList

    for (summary <- summaries) {
      val key = summary.getKey
      if(!key.endsWith("/")) {
        if(throttleConsumption()) {
          dispatcher ! Messages.File(key)
        } else {
          logError("Stopping self as throttleConsumption returned false :(")
        }
      }
    }

    if(listing.isTruncated) {
      listing.setContinuationToken(listing.getNextContinuationToken)
      consumeListingResult(listing)
    }
  }

  def crawlDirectory(): Unit = {
    val request = new ListObjectsV2Request()
      .withBucketName(settings.bucket)
      .withPrefix(directory)
      .withMaxKeys(10000)

    if(recurse) request.setDelimiter("/")

    val listing = s3Client.listObjectsV2(request)
    consumeListingResult(listing)

    dispatcher ! Messages.DirectoryComplete(directory)
    context.stop(self)
  }

  override def receive: Receive = {
    case Messages.Begin    => crawlDirectory()
    case unk: Any => logError(s"Unknown message $unk")
  }
}

object DirectoryCrawlerActor {
  def props(
      s3Client: AmazonS3, directory: String, recurse: Boolean,
      dispatcher: ActorRef, settings: CommandLineFlags): Props =
    Props(new DirectoryCrawlerActor(s3Client, directory, recurse, dispatcher, settings))
}

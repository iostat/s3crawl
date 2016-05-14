package io.stat.s3crawl

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3Client
import io.stat.util.Logging

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/**
  * Created by io on 5/13/16. io is an asshole because
  * he doesn't write documentation for his code.
  *
  * @author Ilya Ostrovskiy (https://github.com/iostat/) 
  */
object Main extends App with Logging {
  CommandLineFlags.parse(args) match {
    case Some(settings) =>
      val s3Client        = new AmazonS3Client(settings.credentials)
      val actorSystem     = ActorSystem("s3crawler")
      val dispatcherActor = actorSystem.actorOf(DispatcherActor.props(s3Client, settings), "dispatcher")
          dispatcherActor ! Messages.SeedDirectory(settings.startChroot)

      implicit val actorSystemExecutor: ExecutionContext = actorSystem.dispatcher

      actorSystem.scheduler.schedule(0 seconds, settings.statusTickTime) {
        dispatcherActor ! Messages.PrintStatus(System.currentTimeMillis())
      }

      Await.result(actorSystem.whenTerminated, Duration.Inf)
    case None =>
      logError("Could not parse command line flags")
      sys.exit(-1)
  }
}

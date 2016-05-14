package io.stat.s3crawl

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor.{Actor, ActorRef, Props}
import com.amazonaws.services.s3.AmazonS3
import io.stat.util.Logging

import scala.collection.mutable.{Queue => MutableQueue, Set => MutableSet}
import scala.util.Random

/**
  * Dispatches crawlers and downloaders as needed
  *
  * @author Ilya Ostrovskiy (https://github.com/iostat/) 
  */
class DispatcherActor(s3Client: AmazonS3, settings: CommandLineFlags)
  extends Actor with Logging {
  val directorySeenCount      = new AtomicInteger(0)
  val fileSeenCount           = new AtomicInteger(0)

  val directoryCrawledCount   = new AtomicInteger(0)
  val fileCopiedCount         = new AtomicInteger(0)

  val directoryQueue          = MutableQueue[String]()
  val fileQueue               = MutableQueue[String]()

  val waitingActorsQueue      = MutableQueue[ActorRef]()
  val workingActors           = new AtomicInteger(0)

  val activeDirectoryCrawlers = MutableSet[ActorRef]()
  val activeFileCrawlers      = MutableSet[ActorRef]()

  val statusTickMilliseconds  = settings.statusTickTime.toMillis
  val lastStatusPrint         = new AtomicLong(0)

  val random                  = new Random()

  def startCrawler(actor: ActorRef) = {
    actor ! Messages.Begin
    workingActors.incrementAndGet()
  }

  def spawnDirectoryCrawler(dir: String, recurse: Boolean = false): ActorRef = {
    val actor = context.actorOf(DirectoryCrawlerActor.props(s3Client, dir, recurse = recurse, self, settings))
    activeDirectoryCrawlers.add(actor)
    waitingActorsQueue.enqueue(actor)
    actor
  }

  def spawnFileCrawler(path: String): ActorRef = {
    val actor =  context.actorOf(FileCrawlerActor.props(s3Client, path, self, settings))
    activeFileCrawlers.add(actor)
    waitingActorsQueue.enqueue(actor)
    actor
  }

  def fillActorPool(): Unit = {
    while(activeDirectoryCrawlers.size < settings.maxCrawlers && directoryQueue.nonEmpty) {
      spawnDirectoryCrawler(directoryQueue.dequeue())
    }

    while(activeFileCrawlers.size < settings.maxDownloaders && fileQueue.nonEmpty) {
      spawnFileCrawler(fileQueue.dequeue())
    }
  }

  def dequeueWaitingActors(): Unit = {
    if(!hasBackPressure) {
      val totalMax   = settings.maxCrawlers + settings.maxDownloaders
      val needed     = totalMax - workingActors.get()
      val backPressureCapacity  = backlogSize.toFloat / settings.maxBacklog.toFloat
      if(needed > 0) {
        val spawnUpTo = Math.ceil(needed.toFloat * (1 - backPressureCapacity)).toInt
        (0 until spawnUpTo) foreach { _ =>
          if(waitingActorsQueue.nonEmpty) {
            startCrawler(waitingActorsQueue.dequeue())
          }
        }
      }
    }
  }

  def stopActorSystemIfCompleted(): Unit = {
    if(activeFileCrawlers.isEmpty && activeDirectoryCrawlers.isEmpty
      && fileQueue.isEmpty && directoryQueue.isEmpty) {
      logInfo("Looks like we're done here!")
    }
  }

  def backlogSize: Int         = directoryQueue.size + fileQueue.size
  def hasBackPressure: Boolean = backlogSize >= settings.maxBacklog

  def printStats(): Unit = {
    def leftPad(str: String, len: Int, chr: Char) = // sorry no npm in scala
      if(str.length < len) {
        "".padTo(len - str.length, chr) + str
      } else str

    val builder = new StringBuilder(s"\nStats\nBacklog: $backlogSize / ${settings.maxBacklog}\n")
    builder append "Crawled / Seen: \n"
    builder append s"    Directory: ${leftPad(directoryCrawledCount.toString, 8, ' ')}/${directorySeenCount.toString.padTo(8, ' ')} | "
    builder append s"Active: ${leftPad(activeDirectoryCrawlers.size.toString, 4, ' ')}/${settings.maxCrawlers}\n"
    builder append s"    File:      ${leftPad(fileCopiedCount.toString, 8, ' ')}/${fileSeenCount.toString.padTo(8, ' ')} | "
    builder append s"Active: ${leftPad(activeFileCrawlers.size.toString, 4, ' ')}/${settings.maxDownloaders}\n"

    logInfo(builder.toString)
  }

  def seenDirectory(dir: String): Unit = {
    directorySeenCount.incrementAndGet()
    directoryQueue.enqueue(dir)
  }

  def seenFile(path: String): Unit = {
    fileSeenCount.incrementAndGet()
    fileQueue.enqueue(path)
  }

  def finishedDirectory(actor: ActorRef): Unit = {
    activeDirectoryCrawlers.remove(actor)
    workingActors.decrementAndGet()
    directoryCrawledCount.incrementAndGet()
  }

  def finishedFile(actor: ActorRef): Unit = {
    activeFileCrawlers.remove(actor)
    workingActors.decrementAndGet()
    fileCopiedCount.incrementAndGet()
  }

  def processMessage(message: Any): Unit = {
    message match {
      case Messages.PhantomDirectory       =>
        directoryCrawledCount.incrementAndGet()
        directorySeenCount.incrementAndGet()

      case Messages.SeedDirectory(dir)     =>
        directorySeenCount.incrementAndGet()
        spawnDirectoryCrawler(dir, recurse = true)
      case Messages.Directory(dir)         => seenDirectory(dir)
      case Messages.File(file)             => seenFile(file)
      case Messages.DirectoryComplete(_)   => finishedDirectory(sender)
      case Messages.FileComplete(_)        => finishedFile(sender)
      case Messages.BackPressureStatus     =>
        if(hasBackPressure) {
          sender ! Messages.PressureExists
        } else {
          sender ! Messages.NoPressure
        }
      case Messages.PrintStatus(timestamp) =>
        if(timestamp - lastStatusPrint.get() >= statusTickMilliseconds) {
          printStats()
          lastStatusPrint.set(System.currentTimeMillis())
        }
      case x: Any => logError(s"Unknown message $x")
    }

    fillActorPool()
    dequeueWaitingActors()
    stopActorSystemIfCompleted()
  }

  override def receive: Receive = { case m => processMessage(m) }
}

object DispatcherActor {
  def props(s3Client: AmazonS3, settings: CommandLineFlags): Props = Props(new DispatcherActor(s3Client, settings))
}

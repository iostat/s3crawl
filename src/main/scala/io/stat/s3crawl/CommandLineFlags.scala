package io.stat.s3crawl

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Case class of command line flags set at startup.
  *
  * @author Ilya Ostrovskiy (https://github.com/iostat/) 
  */
case class CommandLineFlags(
    accessKey:      String,
    secretKey:      String,
    bucket:         String,
    destination:    File,
    maxDownloaders: Int    = 20,
    maxCrawlers:    Int    = 20,
    maxBacklog:     Int    = 1000,
    startChroot:    String = "",
    statusTickTime: FiniteDuration = Duration(1, "seconds")) {

  lazy val credentials = new BasicAWSCredentials(accessKey, secretKey)
}

object CommandLineFlags {
  private[this] val parser = new scopt.OptionParser[CommandLineFlags]("s3crawl") {
    head("s3crawl", "0.1-SNAPSHOT")
    opt[String]('a', "access-key").required valueName "<access key>" action {
      (a, c) => c.copy(accessKey = a)       } text "S3 access key"

    opt[String]('s', "secret-key").required valueName "<secret key>" action {
      (s, c) => c.copy(secretKey = s)       } text "S3 secret key"

    opt[String]('b', "bucket").required     valueName "<bucket>"     action {
      (b, c) => c.copy(bucket = b)          } text "S3 bucket name"

    opt[File]('d', "dest").required         valueName "<path>"       action {
      (d, c) => c.copy(destination = d)     } text "Local destination to sync to"

    opt[Int]('w', "workers")                valueName "<count>"      action {
      (w, c) => c.copy(maxDownloaders =  w) } text "Maximum number of downloader actors (default 20)"

    opt[Int]('c', "crawlers")               valueName "<count>"      action {
      (cr, c) => c.copy(maxCrawlers = cr)   } text "Maximum number of crawler actors (default 20)"

    opt[String]('r', "chroot")              valueName "<chroot>"     action {
      (r, c) => c.copy(startChroot = r)     } text "Location to start crawling from (default is \"\" (root of bucket))"

    opt[Int]('l', "backlog")                valueName "<count>"      action {
      (l, c) => c.copy(maxBacklog = l)      } text "When to start throttling creation of crawlers"

    opt[Int]('t', "status-tick")            valueName "<ms>"         action {
      (t, c) => c.copy(statusTickTime = Duration(t, "ms")) } text "How often to print status (default is 3000)"
  }

  def parse(args: Array[String]): Option[CommandLineFlags] =
    parser.parse(args, CommandLineFlags(null, null, null, null))
}

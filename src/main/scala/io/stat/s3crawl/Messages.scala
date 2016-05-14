package io.stat.s3crawl

/**
  * Messages used by the Actors
  *
  * @author Ilya Ostrovskiy (https://github.com/iostat/) 
  */
object Messages {
  case class SeedDirectory(path: String)
  case class Directory(path: String)
  case class File(path: String)

  case class DirectoryComplete(path: String)
  case class FileComplete(path: String)

  case object PhantomDirectory

  case object Begin
  case class  PrintStatus(timestamp: Long = System.currentTimeMillis())

  case object BackPressureStatus
  case object PressureExists
  case object NoPressure
}

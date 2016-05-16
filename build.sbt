organization         := "io.stat.tools"
name                 := "s3crawl"
version              := "0.3-SNAPSHOT"

scalaVersion         := "2.11.8"
scalacOptions       ++= Seq(
  "-language:postfixOps", "-language:implicitConversions"
)

resolvers            += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"      % "2.4.4",
  "com.github.scopt"  %% "scopt"           % "3.4.0",
  "ch.qos.logback"     % "logback-classic" % "1.1.7",
  "com.amazonaws"      % "aws-java-sdk"    % "1.10.77"
)

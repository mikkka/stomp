organization := "org.mtkachev"

name := "stomp-server"

version := "0.0.1"

scalaVersion := "2.10.0"

libraryDependencies <+= scalaVersion( "org.scala-lang" % "scala-actors" % _ )

libraryDependencies ++= Seq(
  "io.netty" % "netty-common" % "4.0.0.Beta2",
  "io.netty" % "netty-buffer" % "4.0.0.Beta2",
  "io.netty" % "netty-transport" % "4.0.0.Beta2",
  "io.netty" % "netty-codec" % "4.0.0.Beta2",
  "io.netty" % "netty-handler" % "4.0.0.Beta2",
  "org.mockito" % "mockito-core" % "1.9.5" % "test",
  "org.hamcrest" % "hamcrest-library" % "1.1" % "test",
  "org.specs2" %% "specs2" % "1.13" % "test",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

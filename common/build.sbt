organization := "org.mtkachev"

name := "stomp-common"

version := "0.0.1"

scalaVersion := "2.11.1"


libraryDependencies ++= Seq(
  "io.netty" % "netty-common" % "4.0.0.Beta2",
  "io.netty" % "netty-buffer" % "4.0.0.Beta2",
  "io.netty" % "netty-transport" % "4.0.0.Beta2",
  "io.netty" % "netty-codec" % "4.0.0.Beta2",
  "io.netty" % "netty-handler" % "4.0.0.Beta2"
)
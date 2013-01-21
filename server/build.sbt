organization := "org.mtkachev"

name := "stomp-server"

version := "0.0.1"

scalaVersion := "2.10.0"

libraryDependencies <+= scalaVersion( "org.scala-lang" % "scala-actors" % _ )

libraryDependencies ++= Seq(
  "org.jboss.netty" % "netty" % "3.2.6.Final",
  "org.mockito" % "mockito-core" % "1.9.5" % "test",
  "org.hamcrest" % "hamcrest-library" % "1.1" % "test",
  "org.specs2" %% "specs2" % "1.13" % "test"
)

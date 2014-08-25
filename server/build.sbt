organization := "org.mtkachev"

name := "stomp-server"

version := "0.0.1"

scalaVersion := "2.11.1"

libraryDependencies <+= scalaVersion( "org.scala-lang" % "scala-actors" % _ )

libraryDependencies ++= Seq(
  "org.mockito" % "mockito-core" % "1.9.5" % "test",
  "org.hamcrest" % "hamcrest-library" % "1.1" % "test",
  "org.specs2" %% "specs2" % "2.3.12" % "test",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.typesafe" % "config" % "1.2.1",
  "org.scala-lang" %% "scala-pickling" % "0.8.0"
)

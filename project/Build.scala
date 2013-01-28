import sbt._
import Keys._

object HelloBuild extends Build {
  lazy val root = Project(id = "root",
    base = file(".")) aggregate(server)

  lazy val server = Project(id = "stomp-server",
    base = file("server"))
}
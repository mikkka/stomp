import sbt._
import Keys._

object RootBuild extends Build {
  lazy val root = Project(id = "root", base = file(".")).aggregate(common, server, client)

  lazy val common = Project(id = "stomp-common", base = file("common"))

  lazy val server = Project(id = "stomp-server", base = file("server")).dependsOn(common)

  lazy val client = Project(id = "stomp-server", base = file("server")).dependsOn(common)
}
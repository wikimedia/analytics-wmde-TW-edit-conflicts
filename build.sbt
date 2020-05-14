name := "Edit conflict analytics"
organization := "de.wikimedia"
version := "0.2"

scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-deprecation",
  "-Xfatal-warnings",
  "-Xlint",
)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
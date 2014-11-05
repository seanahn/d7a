name := "d7a"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  anorm,
  cache
)     

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"

play.Project.playScalaSettings


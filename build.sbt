name := "d7a"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  anorm,
  cache
)     

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.1.0"

libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "2.0.8"

libraryDependencies += "org.apache.cassandra" % "cassandra-clientutil" % "2.0.8"
 
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"

libraryDependencies += "com.force.api" % "force-wsc" % "32.1.1"

play.Project.playScalaSettings

resourceDirectory in Test <<= (baseDirectory) apply {(baseDir: File) => baseDir / "testResources"}


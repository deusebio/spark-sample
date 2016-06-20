name := "SparkSample"

version := "1.0"

scalaVersion := "2.10.5"

/*
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Cloudera CDH" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers += Resolver.mavenLocal
*/

val sparkVersion = "1.6.1"
val log4jVersion = "1.2.17"
val slf4jVersion = "1.7.21"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "com.databricks" % "spark-csv_2.10" % "1.4.0",
  "log4j" % "log4j" % log4jVersion,
  "org.scalatest" % "scalatest_2.10" % "2.2.6" % "test",
  "junit" % "junit" % "4.12" % "test"
)

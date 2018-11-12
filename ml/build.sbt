name := "machine_learning"

version := "1.0"

scalaVersion := "2.10.5"

val gitHeadCommitSha = taskKey[String]("determine the current git commit SHA")
val makeVersion = taskKey[Seq[File]]("Makes a version.properties file.")
gitHeadCommitSha := Process("git rev-parse HEAD").lines.head
makeVersion := {
  val propFile = (baseDirectory in Compile).value / "version.properties"
  val content = s"version=${gitHeadCommitSha.value}"
  println("path = " + (baseDirectory in Compile).value)
  println(s"version=${gitHeadCommitSha.value}")
  IO.write(propFile, content)
  Seq(propFile)
}

//resolvers += Resolver.mavenLocal
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.0"

    
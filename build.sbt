name := "Assignment1"
version := "1.0"
scalaVersion := "2.12.10"
Compile / mainClass  := Some("com.example.RunSpark")
assembly / mainClass  := Some("com.example.RunSpark")
assembly / assemblyJarName := "Assignment1.jar"

val sparkVersion = "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
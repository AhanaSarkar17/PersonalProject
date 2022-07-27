name := "SparkScalaTestProject"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

val sparkVersion = "3.1.3"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

//lazy val root = (project in file("."))
//  .settings(
//    name := "SparkScalaTestProject"
//  )

assembly / assemblyMergeStrategy := {
  case PathList("META-INF","services", xg @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
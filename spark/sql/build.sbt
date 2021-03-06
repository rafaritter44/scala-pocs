name := "SparkSQL"
version := "1.0.0"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.6"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided", 
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
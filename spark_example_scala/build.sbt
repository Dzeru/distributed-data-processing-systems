name := "spark_example_scala"
version := "1.0.0"
scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.2.0"),
  ("org.apache.spark" %% "spark-sql" % "3.2.0")
)
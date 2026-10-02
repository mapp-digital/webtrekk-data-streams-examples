name := "wt-data-streams-scala-example"

version := "0.1"

scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "4.3.1",
  "org.apache.kafka" % "kafka-clients" % "4.3.1"
)
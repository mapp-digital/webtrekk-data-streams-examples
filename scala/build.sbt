name := "wt-data-streams-scala-example"

version := "0.1"

scalaVersion := "2.12.10"

// Resolver for kafka-avro-serializer
resolvers ++= Seq(
  "Confluent Repository" at "http://packages.confluent.io/maven/", Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % "2.3.0",
  "org.apache.kafka" % "kafka-clients" % "2.3.0"
)
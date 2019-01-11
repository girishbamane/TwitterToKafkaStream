name := "TwitterToKafkaStream"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.1"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.7"
libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "1.1.1"

name := "Robin-DeHaes-SA1"

version := "1.0"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.32",
  "com.typesafe.akka" %% "akka-actor" % "2.5.32",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "2.0.2"
)
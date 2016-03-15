name := """streaming-helpers"""

version := "1.0"

// The Databricks notebooks use Scala 2.10.
//scalaVersion := "2.11.7"
scalaVersion := "2.10.5"

scalacOptions := Seq("-deprecation", "-feature")

mainClass := Some("com.databricks.training.streaming.servers.logs.LogServerMain")

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"      % "1.4.0" % "provided",
  "org.apache.spark"  %% "spark-sql"       % "1.4.0" % "provided",
  "org.apache.spark"  %% "spark-streaming" % "1.4.0" % "provided",
  "com.typesafe.akka" %% "akka-actor"      % "2.3.11",
  "com.typesafe.akka" %% "akka-testkit"    % "2.3.11" % "test",
  "com.typesafe.akka" %% "akka-slf4j"      % "2.3.11",
  "ch.qos.logback"    %  "logback-classic" % "1.0.13",
  "org.scalatest"     %% "scalatest"       % "2.2.4" % "test"
)

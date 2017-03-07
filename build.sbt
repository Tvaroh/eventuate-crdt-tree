import sbt._, Keys._

name := "eventuate-crdt-tree"
organization := "io.treev.eventuate"

scalaVersion := "2.12.1"
scalacOptions := Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Ywarn-unused-import"
)
scalacOptions in (Compile, console) ~= { defaultOptions =>
  val unwantedOptions = Set("-Ywarn-unused-import", "-Xfatal-warnings")
  defaultOptions filterNot unwantedOptions
}

resolvers in ThisBuild ++= Seq(
  "Eventuate Releases" at "https://dl.bintray.com/rbmhtechnology/maven"
)

lazy val IntegrationTest = config("it") extend Test

def itFilter(name: String): Boolean = name contains "ISpec"
def unitFilter(name: String): Boolean = (name contains "Spec") && !itFilter(name)

val testSettings = Seq(
  testOptions in Test := Seq(Tests.Filter(unitFilter)),
  testOptions in IntegrationTest := Seq(Tests.Filter(itFilter)),
  parallelExecution in IntegrationTest := false,
  fork in IntegrationTest := true
)

val AkkaVersion = "2.4.17"
val EventuateVersion = "0.9-M1"

lazy val root =
  project.in(file("."))
    .configs(IntegrationTest)
    .settings(inConfig(IntegrationTest)(Defaults.testTasks): _*)
    .settings(testSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.rbmhtechnology" %% "eventuate-core" % EventuateVersion,
        "com.rbmhtechnology" %% "eventuate-core" % EventuateVersion % Test classifier "it",
        "com.rbmhtechnology" %% "eventuate-crdt" % EventuateVersion,
        "com.rbmhtechnology" %% "eventuate-log-leveldb" % EventuateVersion % Test classifier "" classifier "it",
        "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
        "org.scalatest" %% "scalatest" % "3.0.1" % Test
      )
    )

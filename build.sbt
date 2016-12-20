import sbt._, Keys._

name := "eventuate-crdt-tree"
organization := "io.treev.eventuate"

scalaVersion := "2.12.1"
crossScalaVersions := Seq(scalaVersion.value, "2.11.8")
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

val EventuateVersion = "0.9-SNAPSHOT"

lazy val root =
  project.in(file("."))
    .configs(IntegrationTest)
    .settings(Defaults.itSettings: _*)
    .settings {
      libraryDependencies ++= Seq(
        "com.rbmhtechnology" %% "eventuate-crdt" % EventuateVersion,
        "com.rbmhtechnology" %% "eventuate-log-leveldb" % EventuateVersion % Test classifier "it",
        "org.scalatest" %% "scalatest" % "3.0.1" % Test
      )
    }

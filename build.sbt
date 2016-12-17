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

resolvers in ThisBuild += "Eventuate Releases" at "https://dl.bintray.com/rbmhtechnology/maven"

libraryDependencies += "com.rbmhtechnology" %% "eventuate-crdt" % "0.8.1"

import JmhKeys._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

organization := "github.com/haghard"

name := "mongo-query-streams"

version := "0.4"

scalaVersion := "2.11.5"

// only use a single thread for building
parallelExecution := false

// Execute tests in the current project serially
//   Tests from other projects may still run concurrently.
parallelExecution in Test := false

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-Yno-adapted-args",
  "-target:jvm-1.7"
)

val MongoDriverVersion = "2.13.0"
val ScalazStreamVersion = "0.6a"
val localMvnRepo = "/Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository"

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)

net.virtualvoid.sbt.graph.Plugin.graphSettings

jmhSettings

outputTarget in Jmh := target.value / s"scala-${scalaBinaryVersion.value}"

resolvers += "Local Maven Repository" at "file:///" + localMvnRepo
resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
    "org.mongodb"       %   "mongo-java-driver" %   MongoDriverVersion withSources(),
    "org.scalaz.stream" %%  "scalaz-stream"     %   ScalazStreamVersion withSources(),
    "log4j"             %   "log4j"             %   "1.2.14")

libraryDependencies ++= Seq(
  "de.bwaldvogel"   %   "mongo-java-server" %   "1.1.3",
  "org.scalatest"   %%  "scalatest"         %   "2.2.1",
  "org.scalacheck"  %%  "scalacheck"        %   "1.12.1"    %   "test" exclude("org.scala-lang", "*"),
  "org.specs2"      %%  "specs2"            %   "2.4.14"    %   "test" withSources()
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.7",
  "-deprecation",
  "-unchecked",
  "-Ywarn-dead-code",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:existentials")

javacOptions ++= Seq(
  "-source", "1.7",
  "-target", "1.7",
  "-Xlint:unchecked",
  "-Xlint:deprecation")

//javaHome := Some(file("/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home"))

// Publishing
publishMavenStyle := true

publishTo := Some(Resolver.file("file",  new File(localMvnRepo)))

//eval System.getProperty("java.version")
//eval System.getProperty("java.home")
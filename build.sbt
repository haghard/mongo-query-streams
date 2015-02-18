import JmhKeys._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

organization := "github.com/haghard"

name := "mongo-query-streams"

version := "0.5"

scalaVersion := "2.11.5"

parallelExecution := false
parallelExecution in Test := false
logBuffered in Test := false

initialCommands in console in Test := "import org.specs2._"

//shellPrompt := { state => System.getProperty("user.name") + "> " }

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
val spec2 = "2.4.15"
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
  "de.bwaldvogel"   %   "mongo-java-server"   %   "1.1.3",
  "org.specs2"      %%  "specs2-core"         %   spec2   %   "test",
  "org.specs2"      %%  "specs2-mock"         %   spec2   %   "test",
  "org.specs2"      %%  "specs2-junit"        %   spec2   %   "test",
  "org.specs2"      %%  "specs2-scalacheck"   %   spec2   %   "test",
  "org.specs2"      %%  "specs2"              %   spec2   %   "test" withSources()
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
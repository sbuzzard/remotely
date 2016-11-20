
import common._

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

parallelExecution in Global := false

scalacOptions ++= Seq(
  "-Ywarn-value-discard",
  "-Xlint",
  "-deprecation",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps"
)

libraryDependencies ++= Seq(
  "org.scala-lang.modules"  %% "scala-parser-combinators"  % "1.0.4",
  "org.scodec"              %% "scodec-core"               % "1.10.3",
  "org.typelevel"           %% "cats-core"                 % "0.8.1",
  "co.fs2"                  %% "fs2-core"                  % "0.9.2",
  "co.fs2"                  %% "fs2-cats"                  % "0.2.0",
  "co.fs2"                  %% "fs2-io"                    % "0.9.2",
  "io.netty"                % "netty-handler"              % "4.1.5.Final",
  "io.netty"                % "netty-codec"                % "4.1.5.Final"
)

common.macrosSettings

common.settings

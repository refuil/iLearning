name := "iLearning"

version := "0.1"

scalaVersion := "2.11.0"

javacOptions ++= Seq("-encoding", "UTF-8")

val sparkVersion = "2.4.0"
libraryDependencies += ("org.apache.spark" %% "spark-core" % sparkVersion % "compile" withSources()  intransitive()).
  exclude("commons-beanutils", "commons-beanutils-core").
  exclude("org.mortbay.jetty", "servlet-api").
  exclude("commons-collections", "commons-collections").
  exclude("org.slf4j", "jcl-over-slf4j").
  exclude("com.esotericsoftware.minlog", "minlog").
  excludeAll(
    ExclusionRule(organization = "org.eclipse.jetty.orbit"),
    ExclusionRule(organization = "org.scala-lang")
  )
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"
libraryDependencies += "com.huaban" % "jieba-analysis" % "1.0.2"
libraryDependencies += "org.ansj" % "ansj_seg" % "5.0.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided" withSources()
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources()
libraryDependencies += ("org.apache.spark" %% "spark-hive" % sparkVersion % "provided" withSources())
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.10"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.2.0" withSources()
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources()
libraryDependencies += "com.hankcs" % "hanlp" % "portable-1.6.2"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0"
libraryDependencies += "org.jblas" % "jblas" % "1.2.4" % "provided" withSources()
libraryDependencies += "net.librec" % "librec-core" % "2.0.0" % "provided" withSources()

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"1.6.1_0.7.0" % "test")
libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-jackson" % "3.2.10" withSources(),
  "org.codehaus.jettison" % "jettison" % "1.1",
  "org.ini4j" % "ini4j" % "0.5.4"
)
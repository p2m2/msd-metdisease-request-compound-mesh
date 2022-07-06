ThisBuild / scalaVersion     := "2.12.16"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.github.p2m2"
ThisBuild / organizationName := "p2m2"

val sparkVersion  = "3.2.1"
lazy val rdf4jVersion = "4.0.2"
lazy val slf4j_version = "1.7.36"

lazy val root = (project in file("."))
  .settings(
    name := "msd-metdisease-request-compound-mesh",

		libraryDependencies ++= Seq(
      "com.lihaoyi" %% "utest" % "0.8.0" % "test",
      ("org.apache.spark" %% "spark-sql"  % sparkVersion % "provided,test")
        .exclude("com.fasterxml.jackson","databind"),
      ("net.sansa-stack" %% "sansa-rdf-spark" % "0.8.0-RC3")
        .exclude("org.apache.zookeeper","zookeeper")
        .exclude("org.apache.avro","avro-mapred")
        .exclude("com.fasterxml.jackson","databind")
        .exclude("org.apache.hadoop","hadoop-common") % "test,provided",
      ("net.sansa-stack" %% "sansa-ml-spark" % "0.8.0-RC3")
        .exclude("org.apache.zookeeper","zookeeper")
        .exclude("org.apache.avro","avro-mapred")
        .exclude("com.fasterxml.jackson","databind")
        .exclude("org.apache.hadoop","hadoop-common") % "test,provided"
      ,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3" % "test",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.13.3" % "test",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.3" % "test",
      "com.lihaoyi" %% "requests" % "0.7.1",
      "com.github.scopt" %% "scopt" % "4.0.1",
      "com.github.p2m2" %% "service-rdf-database-deployment" % "1.0.12"
    ),
    Compile / mainClass := Some("fr.inrae.msd.rdf.RequestCompoundBuilder"),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    resolvers ++= Seq(
      "AKSW Maven Releases" at "https://maven.aksw.org/archiva/repository/internal",
      "AKSW Maven Snapshots" at "https://maven.aksw.org/archiva/repository/snapshots",
      "oss-sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Apache repository (snapshots)" at "https://repository.apache.org/content/repositories/snapshots/",
      "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/", "NetBeans" at "https://bits.netbeans.org/nexus/content/groups/netbeans/", "gephi" at "https://raw.github.com/gephi/gephi/mvn-thirdparty-repo/",
      Resolver.defaultLocal,
      Resolver.mavenLocal,
      "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
    ),
    testFrameworks += new TestFramework("utest.runner.Framework"),
    assembly / target := file("assembly"),
    assembly / assemblyJarName := s"msd-metdisease-request-compound-mesh.jar",
    assembly / logLevel := Level.Info,
    assembly / assemblyMergeStrategy := {
     //case PathList("META-INF", xs @ _*) => MergeStrategy.last
      case "META-INF/io.netty.versions.properties" => MergeStrategy.first
      case "META-INF/versions/9/module-info.class" => MergeStrategy.first
      case "module-info.class"  => MergeStrategy.first
      //case x if x.endsWith("Messages.properties")  => MergeStrategy.first
      case x if x.endsWith(".properties")  => MergeStrategy.discard
      case x if x.endsWith(".ttl")  => MergeStrategy.first
      case x if x.endsWith(".nt")  => MergeStrategy.first
      case x if x.endsWith(".txt")  => MergeStrategy.discard
      case x if x.endsWith(".class") ||
        x.endsWith("plugin.xml") ||
        x.endsWith(".res") ||
        x.endsWith(".xsd") ||
        x.endsWith(".proto") ||
        x.endsWith(".dtd")  ||
        x.endsWith(".ExtensionModule")=> MergeStrategy.first
      case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

Global / onChangedBuildSource := ReloadOnSourceChanges

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

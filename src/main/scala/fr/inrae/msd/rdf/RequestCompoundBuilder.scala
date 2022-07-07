package fr.inrae.msd.rdf

import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, SparkSession}

import java.util.Date

object RequestCompoundBuilder extends App {

  import scopt.OParser

  case class Config(
                     rootMsdDirectory : String = "/rdf",
                     forumCategoryMsd : String = "forum/DiseaseChem",
                     forumDatabaseMsd : String = "PMID_CID",
                     forumVersionMsd : String = "test",
                     verbose: Boolean = false,
                     debug: Boolean = false)

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("msd-metdisease-database-pmid-cid-builder"),
      head("msd-metdisease-database-pmid-cid-builder", "1.0"),
      opt[String]('d', "rootMsdDirectory")
        .optional()
        .valueName("<rootMsdDirectory>")
        .action((x, c) => c.copy(rootMsdDirectory = x))
        .text("versionMsd : release of reference/pubchem database"),
      opt[String]('r', "versionMsd")
        .optional()
        .valueName("<versionMsd>")
        .action((x, c) => c.copy(forumVersionMsd = (x)))
        .text("versionMsd : release of reference/pubchem database"),
      opt[Unit]("verbose")
        .optional()
        .action((_, c) => c.copy(verbose = true))
        .text("verbose is a flag"),
      opt[Unit]("debug")
        .hidden()
        .action((_, c) => c.copy(debug = true))
        .text("this option is hidden in the usage text"),

      help("help").text("prints this usage text"),
      note("some notes." + sys.props("line.separator")),
      checkConfig(_ => success)
    )
  }
  val spark = SparkSession
    .builder()
    .appName("msd-metdisease-request-compound-mesh")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.kryo.registrator","net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    .getOrCreate()


    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        // do something
        println(config)
        build(
          config.rootMsdDirectory,
          config.forumCategoryMsd,
          config.forumDatabaseMsd,
          config.verbose,
          config.debug)
      case _ =>
        // arguments are bad, error message will have been displayed
        System.err.println("exit with error.")
    }


  /**
   * First execution of the work.
   * Build asso PMID <-> CID and a list f PMID error
   * @param rootMsdDirectory
   * @param forumCategoryMsd
   * @param forumDatabaseMsd
   * @param verbose
   * @param debug
   */
  def build(
             rootMsdDirectory : String,
             forumCategoryMsd : String,
             forumDatabaseMsd : String,
             verbose: Boolean,
             debug: Boolean) : Unit = {

    val startBuild = new Date()
    val chebiPath        : String = "/rdf/ebi/chebi/13-Jun-2022/chebi.owl"
    val compoundTypePath : String = "/rdf/pubchem/compound-general/2022-06-08/pc_compound_type.ttl"

    // /rdf/pubchem/reference/2022-06-08/pc_reference2meshheading_000001.ttl ==> fabio:hasSubjectTerm
    // mesh ==> meshv:hasDescriptor/meshv:treeNumber

    val triplesDataset : Dataset[Triple] =
      spark.rdf(Lang.RDFXML)(chebiPath).toDS()
        .union(spark.rdf(Lang.TURTLE)(compoundTypePath).toDS())

    ChebiWithOntoMeshUsedThesaurus(spark)
      .getChebiIDLinkedWithCID(triplesDataset,20)
      .write
      .format("text")
      .mode("overwrite")
      .save("test_chebi_cid.txt")



    println("FIN")
/*
    val contentProvenanceRDF : String =
      ProvenanceBuilder.provSparkSubmit(
      projectUrl ="https://github.com/p2m2/msd-metdisease-request-compound-mesh",
      category = forumCategoryMsd,
      database = forumDatabaseMsd,
      release="test",
      startDate = startBuild,
      spark
    )

    MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category="prov",
      database="",
      version=Some("")).writeFile(spark,contentProvenanceRDF,"msd-metdisease-request-compound-mesh-test.ttl")

    spark.close()*/
  }

}

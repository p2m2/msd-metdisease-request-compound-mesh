package fr.inrae.msd.rdf

import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import utest.{TestSuite, Tests, test}

object ChebiCompoundTest extends TestSuite{
  val spark: SparkSession = Config.spark

  val meshPath : String = "rdf/nlm/mesh/current_release/mesh.nt"
  val meshVocabPath : String = "rdf/nlm/mesh/current_release/mesh.nt"
  val chebiPath        : String = "./rdf/ebi/chebi/current_release/chebi.owl"
  val compoundTypePath : String = "./rdf/pubchem/compound-general/current_release/pc_compound_type.ttl"
  val referenceTypePath : String = "./rdf/pubchem/reference/current_release/pc_compound_type.ttl"
  val pmidCidPath : String = "./rdf/forum/DiseaseChem/PMID_CID/test_2022-07-07-105456/pmid_cid.ttl"
  val pmidCidEndpointPath : String = "./rdf/forum/DiseaseChem/PMID_CID/test_2022-07-07-105456/pmid_cid_endpoints.ttl"
  val citoPath : String = "./rdf/vocabularies/cito.ttl"
  val fabioPath : String = "./rdf/vocabularies/fabio.ttl"

  val triplesDataset : RDD[Triple] =
    spark.rdf(Lang.RDFXML)(chebiPath)
      .union(spark.rdf(Lang.TURTLE)(compoundTypePath))

  val triplesDataset2 : RDD[Triple] =
    spark
      .rdf(Lang.TURTLE)(pmidCidPath)
   //   .union(spark.rdf(Lang.TURTLE)(pmidCidEndpointPath).toDS())
  //    .union(spark.rdf(Lang.TURTLE)(compoundTypePath).toDS())
      .union(spark.rdf(Lang.TURTLE)(citoPath))
      .union(spark.rdf(Lang.TURTLE)(fabioPath))
   //   .union(spark.rdf(Lang.NT)(meshPath).toDS())

  val tests: Tests = Tests {

     test("getSubjectFromRecursiveProperty Level base"){
       val work = ChebiWithOntoMeshUsedThesaurus(spark)
      /*
       ChebiWithOntoMeshUsedThesaurus(spark)
         .getChebiIDLinkedWithCID(triplesDataset,4)*/
      // work.getChebiCount(work.applyInferenceAndSaveTriplets(triplesDataset2,"owl_inf"))

       ChebiWithOntoMeshUsedThesaurus(spark)
         .applyInferenceAndSaveTriplets(triplesDataset2,"test")
         .saveAsNTriplesFile("./rdf/request/forum-inference-CHEBI-PMID.nt")
     }
  }
}

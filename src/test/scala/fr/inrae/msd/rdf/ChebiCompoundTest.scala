package fr.inrae.msd.rdf

import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, SparkSession}
import utest.{TestSuite, Tests, test}

object ChebiCompoundTest extends TestSuite{
  val spark: SparkSession = Config.spark

  val chebiPath        : String = "./rdf/ebi/chebi/current_release/chebi.owl"
  val compoundTypePath : String = "./rdf/pubchem/compound-general/current_release/pc_compound_type.ttl"


  val triplesDataset : Dataset[Triple] =
    spark.rdf(Lang.RDFXML)(chebiPath).toDS()
      .union(spark.rdf(Lang.TURTLE)(compoundTypePath).toDS())

  val tests = Tests {

     test("getSubjectFromRecursiveProperty Level base"){
       ChebiWithOntoMeshUsedThesaurus(spark)
         .getChebiIDLinkedWithCID(triplesDataset,4)
     }
  }
}

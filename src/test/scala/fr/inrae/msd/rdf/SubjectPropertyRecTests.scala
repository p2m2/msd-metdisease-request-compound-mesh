package fr.inrae.msd.rdf

import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, SparkSession}
import utest._

object SubjectPropertyRecTests extends TestSuite {

  val spark: SparkSession = Config.spark

  val compoundTypePath : String = "./rdf/pubchem/compound-general/current_release/pc_compound_type.ttl"

  val triplesDataset : Dataset[Triple] = spark.rdf(Lang.TURTLE)(compoundTypePath).toDS()

  val tests: Tests = Tests{

    test("") {
    }
/*
    test("getSubjectFromRecursiveProperty Level 3"){
      val lP  = work
        .getSubjectFromRecursiveProperty(chebiTriplesDataset,
          "rdfs:subClassOf","chebi:24431",level=3).collect()

        lP.map(
          chebi => {
            ChebiWithOntoMeshUsedThesaurus(spark)
              .getSubjectFromRecursiveProperty(
                compoundTypeTriplesDataset,
                "a",
                s"<${chebi.toString}>",level=3)
          }
        )
    }*/
  }
}

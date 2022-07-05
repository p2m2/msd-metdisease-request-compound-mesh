package fr.inrae.msd.rdf
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.Lang
import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

case object ChebiWithOntoMeshUsedThesaurus {

  val count_distinct_pmids_by_ChEBI = """
select ?CHEBI ?count
%s
where
{
    {
        select ?CHEBI ?count
        where
        {
            {
                select (strafter(STR(?chebi),\"http://purl.obolibrary.org/obo/CHEBI_\") as ?CHEBI) (count(distinct ?pmid) as ?count)
                where
                {
                    {
                        select ?chebi
                        where
                        {
                            {
                                select distinct ?chebi where
                                {
                                    ?chebi rdfs:subClassOf+ chebi:24431 .
                                    ?cid a+ ?chebi
                                }
                                group by ?chebi
                                having(count (distinct ?cid) <= 1000 && count(distinct ?cid) > 1)
                                order by ?chebi
                            }
                        }
                    }
                    ?cid a+ ?chebi .
                    ?cid cito:isDiscussedBy ?pmid .
                    ?pmid (fabio:hasSubjectTerm/meshv:treeNumber|fabio:hasSubjectTerm/meshv:hasDescriptor/meshv:treeNumber) ?tn .
                    FILTER(REGEX(?tn,\"(C|A|D|G|B|F|I|J)\")) .
                    ?mesh meshv:treeNumber ?tn .
                    ?mesh a meshv:TopicalDescriptor .
                    ?mesh meshv:active 1 .
                }
                group by ?chebi
            }
        }
        order by ?CHEBI
    }
}
"""

  val prefixes : Map[String,String] = Map(
    "rdf" ->"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs" -> "http://www.w3.org/2000/01/rdf-schema#",
    "owl" -> "http://www.w3.org/2002/07/owl#",
    "meshv" -> "http://id.nlm.nih.gov/mesh/vocab#",
    "chebi" -> "http://purl.obolibrary.org/obo/CHEBI_"
  )

  def test1(spark : SparkSession) = {
    val queryString =
      prefixes.map { case (key,value) => "PREFIX "+key+":<"+value+"> "}.mkString("\n")+
      """
        |select distinct ?chebi where
        |                                {
        |                                    ?chebi rdfs:subClassOf+ chebi:24431 .
        |                                    ?cid a+ ?chebi
        |                                }
        |                                group by ?chebi
        |                                having(count (distinct ?cid) <= 1000 && count(distinct ?cid) > 1)
        |                                order by ?chebi
        |""".stripMargin

    val meshPath = "./rdf/nlm/mesh/current_release/mesh.nt"

    val chebiPath = "./rdf/ebi/chebi/current_release/chebi.owl"

    val triplesMesh : RDD[Triple] = spark.rdf(Lang.TURTLE)(chebiPath)
    val triplesDataset : Dataset[Triple] = triplesMesh.toDS()

    val sparqlFrame =
      new SparqlFrame()
        .setSparqlQuery(queryString)
        .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    implicit val enc: Encoder[String] = Encoders.STRING

    sparqlFrame.transform(triplesDataset).map(
      row  => row.get(0).toString
    ).rdd
  }

}

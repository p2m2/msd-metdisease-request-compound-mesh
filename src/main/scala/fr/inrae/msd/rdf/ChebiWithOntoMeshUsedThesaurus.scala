package fr.inrae.msd.rdf

import net.sansa_stack.inference.rules.RDFSLevel
import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.inference.spark.forwardchaining.triples.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.util.{Failure, Success, Try}

case class ChebiWithOntoMeshUsedThesaurus(spark : SparkSession) {

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
    "chebi" -> "http://purl.obolibrary.org/obo/CHEBI_",
    "oboInOwl" -> "http://www.geneontology.org/formats/oboInOwl#",
    "cito" -> "http://purl.org/spar/cito/",
    "fabio"-> "http://purl.org/spar/fabio/"
  )

  implicit val nodeEncoder: Encoder[Node] = Encoders.kryo(classOf[Node])

  def getPrefixSparql : String = prefixes.map { case (key,value) => "PREFIX "+key+":<"+value+"> "}.mkString("\n")+"\n"

  def applyInferenceAndSaveTriplets(triples : RDD[Triple], idName : String): RDD[Triple] = {

    val t = new ForwardRuleReasonerOWLHorst(spark.sparkContext).apply(triples)
    //val r = new ForwardRuleReasonerRDFS(spark.sparkContext)
    //r.level = RDFSLevel.SIMPLE
    //val t2 = r.apply(t)

    t.toDS().coalesce(64).rdd
  }

  /**
   * TODO Voir avec Maxime
   * Sachant la config des graphes du projet original :
   * graph_from = https://forum.semantic-metabolomics.org/PMID_CID/2021
             https://forum.semantic-metabolomics.org/PMID_CID_endpoints/2021
             https://forum.semantic-metabolomics.org/PubChem/reference/2022-01-01
             https://forum.semantic-metabolomics.org/MeSHRDF/2022-01-04
             https://forum.semantic-metabolomics.org/PubChem/compound/2022-01-01
             https://forum.semantic-metabolomics.org/ChEBI/2021-11-03

   * @param triplesDataset
   * @return
   */
  def getChebiCount(triplesDataset : Dataset[Triple]) : Dataset[String]  = {
    val queryString_complete =
      getPrefixSparql+
        s"""select (strafter(STR(?chebi),"http://purl.obolibrary.org/obo/CHEBI_") as ?CHEBI) (count(distinct ?pmid) as ?count)
                where {
                ?cid a ?chebi .
                FILTER (strstarts(str(?chebi),"http://purl.obolibrary.org/obo/CHEBI_"))
                ?cid cito:isDiscussedBy ?pmid .
                { ?pmid fabio:hasSubjectTerm [ meshv:treeNumber ?tn ] .}
                union
                { ?pmid  fabio:hasSubjectTerm [ meshv:hasDescriptor [ meshv:treeNumber ?tn ] ] . }
                FILTER(REGEX(?tn,"(C|A|D|G|B|F|I|J)")) .
                ?mesh meshv:treeNumber ?tn .
                ?mesh a meshv:TopicalDescriptor .
                ?mesh meshv:active 1 .
                ?cid a ?v0 .
            } group by ?chebi""".stripMargin

//DEFINE input:inference "schema-inference-rules"
    /**
     *  ?cid a ?chebi .
        FILTER (strstarts(str(?chebi),"http://purl.obolibrary.org/obo/CHEBI_"))
        ?cid cito:isDiscussedBy ?pmid .
     */
    val queryString =  getPrefixSparql+
    """PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
       select ?chebi ?tn
       where {
        ?cid a ?chebi .
        FILTER (strstarts(str(?chebi),"http://purl.obolibrary.org/obo/CHEBI_"))
        ?cid cito:isDiscussedBy ?pmid .
        { ?pmid fabio:hasSubjectTerm [ meshv:treeNumber ?tn ] .}
                union
        { ?pmid  fabio:hasSubjectTerm [ meshv:hasDescriptor [ meshv:treeNumber ?tn ] ] . }
       } limit 10""".stripMargin

    println(queryString)

    val sparqlFrame =
      new SparqlFrame()
        .setSparqlQuery(queryString)
        .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

        /*
        val chebiPath="/rdf/ebi/chebi/13-Jun-2022/chebi.owl"
        val compoundPath="/rdf/pubchem/compound-general/2022-06-08/pc_compound_type.ttl"
        val pmid=""
         */

    Try(sparqlFrame.transform(triplesDataset).map(
      row  => row.get(0).toString
    )(Encoders.STRING)) match {
      case Success(value) => value
      case Failure(e) => throw e
    }
  }

  def getChebiIDLinkedWithCIDLevel(triplesDataset : Dataset[Triple],
                              level : Int = 1) : Dataset[Node]  = {

    if ( level < 1) {
      spark.emptyDataset[Node]
    } else {
      val listPredicates = Array.range(0, level)
        .map(
            x => s"?v$x rdfs:subClassOf ?v${x+1} ."
        )
        .mkString("\n")
        .replace( s"?v${level}","chebi:24431" )

      val queryString =
        getPrefixSparql+
          s"""select distinct ?v0 where {
                $listPredicates
                ?cid a ?v0 .
            }""".stripMargin

      println(queryString)

      val sparqlFrame =
        new SparqlFrame()
          .setSparqlQuery(queryString)
          .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)


      Try(sparqlFrame.transform(triplesDataset).map(
        row  => NodeFactory.createURI(row.get(0).toString)
      )) match {
        case Success(value) => value
        case Failure(_) => spark.emptyDataset[Node]
      }
    }

  }

  final def getChebiIDLinkedWithCID(
                                   triplesDataset : Dataset[Triple],
                                   maxDeepSearch : Int = 10,
                                   currentDeep : Int = 1) : Dataset[Node] = {

    if (maxDeepSearch < currentDeep ) {
      spark.emptyDataset[Node]
    } else {
      val current : Dataset[Node] = getChebiIDLinkedWithCIDLevel(triplesDataset,currentDeep)
      current.union(getChebiIDLinkedWithCID(triplesDataset,maxDeepSearch,currentDeep+1))
    }
  }


}

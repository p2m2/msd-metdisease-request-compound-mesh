package fr.inrae.msd.rdf

import org.apache.spark.sql.SparkSession

case object Config {
  val spark = SparkSession
    .builder()
    .appName("msd-metdisease-request-compound-mesh-tests")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.kryo.registrator","net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    .config("spark.testing.memory", "2147480000")
    .config("spark.executor.instances", "4")
    .getOrCreate()
}

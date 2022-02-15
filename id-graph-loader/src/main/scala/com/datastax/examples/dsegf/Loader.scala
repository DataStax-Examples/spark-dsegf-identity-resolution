package com.datastax.examples.dsegf

import com.datastax.bdp.graph.spark.graphframe._
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

import scala.io.Source

object Loader {
  val identifierSchema = new StructType()
    .add("idValue", StringType, nullable = false)
    .add("idType", StringType, nullable = false)
    .add("createDate", DateType, nullable = false)
    .add("countryList", ArrayType(StringType), nullable = false)
    .add("dpList", ArrayType(IntegerType), nullable = false)
    .add("deviceOs", ArrayType(StringType), nullable = false)

  val linkSchema = new StructType()
    .add("idValue1", StringType, nullable = false)
    .add("idType1", StringType, nullable = false)
    .add("idValue2", StringType, nullable = false)
    .add("idType2", StringType, nullable = false)
    .add("dpList", ArrayType(IntegerType), nullable = false)
    .add("quality", DoubleType, nullable = false)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: spark-submit .... <vertexJsonPath> <edgeJsonPath>")
      System.exit(1)
    }

    val vertexPath = args(0).trim
    val edgePath = args(1).trim

    val graphName = "identity_graph_core_engine"

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Graph Load Application")
      .getOrCreate()

    setupIDGSchema(graphName, spark)

    val g = spark.dseGraph(graphName)

    val identifierDF = spark.read.schema(identifierSchema).option("multiline", "true").json(vertexPath).withColumn("~label", lit("idNode"))

    // DSE Graph core engine doesn't support BIDIRECTIONAL edges by default and we need to setup indexes
    // to optimize the BIDIRECTIONAL traversal
    // An alternative is setup the opposite direction edge in application layer as below
    val linkDF = spark.read.schema(linkSchema).option("multiline", "true").json(edgePath)
    val oppDirectionLinkDF = linkDF
      .withColumnRenamed("idValue1", "idv1")
      .withColumnRenamed("idType1", "idt1")
      .withColumnRenamed("idValue2", "idValue1")
      .withColumnRenamed("idType2", "idType1")
      .withColumnRenamed("idv1", "idValue2")
      .withColumnRenamed("idt1", "idType2")
      .select(linkSchema.fields.map(f => col(f.name)): _*)

    // Convert the linkDF to DSE compatible edge DF
    val dseDFCompatibleEdgeDF = linkDF.union(oppDirectionLinkDF)
      .withColumn("src", g.idColumn(lit("idNode"), col("idValue1"), col("idType1")))
      .withColumn("~label", lit("link"))
      .withColumn("dst", g.idColumn(lit("idNode"), col("idValue2"), col("idType2")))

    identifierDF.show(100, false)
    dseDFCompatibleEdgeDF.show(100, false)

    // insert the V & E into DSE core graph
    // note updateVertices
    g.updateVertices(identifierDF)
    g.updateEdges(dseDFCompatibleEdgeDF)

    g.V.show(false)
    g.E.show(false)
  }

  def setupIDGSchema(graphName: String, spark: SparkSession): Unit = {
    val session = CassandraConnector(spark.sparkContext).withSessionDo(session => {
      session.execute(ScriptGraphStatement.builder(s"system.graph('$graphName').ifExists().drop()").build())
      session.execute(ScriptGraphStatement.builder(s"system.graph('$graphName').ifNotExists().create()").build())
      val schema = Source.fromInputStream(getClass.getResourceAsStream("/coreGraphSchema.txt")).getLines.mkString
      session.execute(ScriptGraphStatement.builder(schema).setGraphName(graphName).build())
    })
  }
}

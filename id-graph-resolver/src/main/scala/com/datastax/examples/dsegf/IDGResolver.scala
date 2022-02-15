package com.datastax.examples.dsegf

import com.datastax.bdp.graph.spark.graphframe._
import com.datastax.examples.dsegf.resolvers.ConnectedComponentBasedResolver
import org.apache.spark.sql.SparkSession

object IDGResolver {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: spark-submit .... <toBeMatchedIDsPath.csv> <resolvedIDsOutputPath.csv>")
      System.exit(1)
    }

    val inputIDsPath = args(0).trim
    val resolvedIDsOutputPath = args(1).trim

    val graphName = "identity_graph_core_engine"

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Graph Load Application")
      .getOrCreate()

    val inputDF = spark.read.schema(Const.inputIDsDFSchema).option("header", true).format("csv").load(inputIDsPath)

    val idGF = spark.dseGraph(graphName)

    val resolvedDF = ConnectedComponentBasedResolver.getResolvedDF(inputDF, idGF)(spark)

    println("Input Match Distribution")
    Aggregators.getInputMatchRates(resolvedDF)(spark).show(false)

    println("Resolved ID Match Distribution By IDType")
    Aggregators.getOutputMatchRates(resolvedDF)(spark).show(false)

    resolvedDF.coalesce(1).write.mode("overwrite").option("header", true).csv(resolvedIDsOutputPath)
  }
}

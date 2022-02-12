package com.datastax.examples.dsegf.resolvers

import com.datastax.bdp.graph.spark.graphframe.DseGraphFrame
import com.datastax.examples.dsegf.Const
import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * - ConnectedComponentBasedResolver honors the `country` filter,
 * it outputs only those matching IDs which meet the `country` criteria
 * for each matched ID in the inputDF
 *
 * - ConnectedComponentBasedResolver doesnt honor `qualityHigherThan`
 * filter as ConnectedComponentBasedResolver uses CC representation
 * and uses all the edges as its input (rather than only edges above
 * certain quality threshold)
 *
 * - input is a DF of below fmt
 * +-----------+---------------+---------------+-----------------+-------+
 * |idValue    |idType         |matchType      |qualityHigherThan|country|
 * +-----------+---------------+---------------+-----------------+-------+
 * |NI         |device_id      |appnexus_cookie|0.7              |FRA    |
 * => find all the IDs that can be reached transitively from `NI:device_id`
 * and are of type `appnexus_cookie` and belong to country `FRA` (france)
 *
 * - returns a DF of the following format
 * +-------+-------------+--------------+---------------+-------+---------+
 * |idValue|idType       |matchedIDValue|matchedIDType  |country|dpList   |
 * +-------+-------------+--------------+---------------+-------+---------+
 * |NI     |device_id    |OwSs          |appnexus_cookie|FRA    |[1, 4, 5]|
 * |NI     |device_id    |yUQE          |appnexus_cookie|FRA    |[3, 4, 5]|
 * |NI     |device_id    |wKzp          |appnexus_cookie|FRA    |[1, 4, 5]|
 * => IDs matching the criteria resolved via IDG
 * - dpList -> list of sources which shared this connected knowledge
 *
 */
object ConnectedComponentBasedResolver {
  def getResolvedDF(inputDF: DataFrame, idg: DseGraphFrame)(spark: SparkSession): DataFrame = {
    spark.sparkContext.setCheckpointDir("file:///tmp/ccDir")

    val ccDF = idg.connectedComponents.run()

    // find out the matching components for the IDs in the inputDF
    val ccsubDF = ccDF.select("component", "idValue", "idType").toDF("component", "idv", "idt")
    val joinedDF = ccsubDF.join(inputDF, inputDF("idValue") === ccsubDF("idv") && inputDF("idType") === ccsubDF("idt"), "right").drop("idt", "idv")

    // find out all the IDs in the matched components
    val ccsubWithMetaDF = ccDF.select("component", "idValue", "idType", "countryList", "dpList")
      .toDF("componentCC", "idv", "idt", "countryList", "dpList")
    val allIDsFromMatchingComponentsDF = ccsubWithMetaDF.join(joinedDF, joinedDF("component") === ccsubWithMetaDF("componentCC"))

    // apply the matching criteria
    val matchingIDsFromMatchingComponentsDF = allIDsFromMatchingComponentsDF
      .filter((col("matchType") === col("idt") && array_contains(col("countryList"), col("country")))
        || col("component").isNull)

    matchingIDsFromMatchingComponentsDF
      .select("component", "idv", "idt", "idValue", "idType", "idv", "idt", "country", "dpList")
      .toDF(Const.resolvedDFSchema.fields.map(_.name): _*)
      .drop("dpList")
  }
}

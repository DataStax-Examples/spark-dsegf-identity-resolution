package com.datastax.examples.dsegf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col, when, countDistinct, concat}

object Aggregators {
  /**
   *
   * @param resolvedDF
   * @return
   * +----------------+-----+
   * |is_input_matched|count|
   * +----------------+-----+
   * |true            |4    |
   * |false           |2    |
   * +----------------+-----+
   * => match distribution for the input IDs
   */
  def getInputMatchRates(resolvedDF: DataFrame)(spark: SparkSession): DataFrame = {
    resolvedDF.withColumn("is_input_matched", when(col("component").isNull, false).otherwise(true))
      .groupBy("is_input_matched")
      .agg(countDistinct(concat(col("idValue"), col("idType"))).as("count"))
  }

  /**
   *
   * @param resolvedDF
   * @return
   * +---------------+----------------------+
   * |idType         |output_matched_by_type|
   * +---------------+----------------------+
   * |device_id      |15                    |
   * |google_cookie  |3                     |
   * +---------------+----------------------+
   * => distribution of matched IDs by type
   */
  def getOutputMatchRates(resolvedDF: DataFrame)(spark: SparkSession): DataFrame = {
    resolvedDF.filter(col("component").isNotNull)
      .groupBy("idType")
      .agg(countDistinct(concat(col("idv"),col("idt"))).as("output_matched_by_type"))
  }
}

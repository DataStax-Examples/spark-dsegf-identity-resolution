package com.datastax.examples.dsegf

import org.apache.spark.sql.types.{DoubleType, StringType, StructType, ArrayType, IntegerType}

object Const {
  val inputIDsDFSchema = new StructType()
    .add("idValue", StringType, nullable = false)
    .add("idType", StringType, nullable = false)
    .add("matchType", StringType, nullable = false)
    .add("qualityHigherThan", DoubleType, nullable = false)
    .add("country", StringType, nullable = false)

  val resolvedDFSchema = new StructType()
    .add("component", IntegerType, nullable = false)
    .add("idv", StringType, nullable = false)
    .add("idt", StringType, nullable = false)
    .add("idValue", StringType, nullable = false)
    .add("idType", StringType, nullable = false)
    .add("matchedIDValue", StringType, nullable = false)
    .add("matchedIDType", StringType, nullable = false)
    .add("country", StringType, nullable = false)
    .add("dpList", ArrayType(IntegerType), nullable = false)
}

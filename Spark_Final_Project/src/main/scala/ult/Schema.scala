package ult

import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, StringType, StructType, TimestampType}

object Schema {
  val tranTypeSchema = new StructType()
    .add("trantype", IntegerType, nullable = true)
    .add("transtypename", StringType, nullable = true)

  val genderSchema = new StructType()
    .add("gender", IntegerType, nullable = true)
    .add("genderName", StringType, nullable = true)

  val userSchema = new StructType()
    .add("userId", IntegerType, nullable = true)
    .add("birthdate", DateType, nullable = true)
    .add("profileLevel", IntegerType, nullable = true)
    .add("gender", IntegerType, nullable = true)
    .add("updatedTime", TimestampType, nullable = true)

  val promotionSchema = new StructType()
    .add("customer_id", IntegerType, nullable = true)
    .add("voucherCode", StringType, nullable = true)
    .add("status", StringType, nullable = true)
    .add("campaignID", IntegerType, nullable = true)
    .add("time", TimestampType, nullable = true)

  val transactionSchema = new StructType()
    .add("transId", StringType, nullable = true)
    .add("transStatus", IntegerType, nullable = true)
    .add("userId", IntegerType, nullable = true)
    .add("transactionTime", TimestampType, nullable = true)
    .add("appId", IntegerType, nullable = true)
    .add("transType", IntegerType, nullable = true)
    .add("amount", IntegerType, nullable = true)
    .add("pmcId", IntegerType, nullable = true)

  val campaignSchema = new StructType()
    .add("campaignID", IntegerType, nullable = true)
    .add("campaignType", IntegerType, nullable = true)
    .add("expireDate", TimestampType, nullable = true)
    .add("expireTime", IntegerType, nullable = true)

  val dbActivitySchema = new StructType()
    .add("_id", IntegerType, nullable = true)
    .add("FirstActiveDate", DateType, nullable = true)
    .add("FirstPayDate", DateType, nullable = true)
    .add("LastActiveDate", DateType, nullable = true)
    .add("LastPayDate", DateType, nullable = true)
    .add("lastActiveTransactionType", IntegerType, nullable = true)
    .add("lastPayAppId", IntegerType, nullable = true)
    .add("pmcIds", ArrayType(IntegerType), nullable = true)
    .add("appIds", ArrayType(IntegerType), nullable = true)

  val dbDemographicSchema = new StructType()
    .add("_id", IntegerType, nullable = true)
    .add("age", IntegerType, nullable = true)
    .add("profileLevel", IntegerType, nullable = true)
    .add("gender", StringType, nullable = true)
    .add("updatedTime", TimestampType, nullable = true)

  val db_promo_schema = new StructType()
    .add("_id", StringType, nullable = true)
    .add("ValidDate", TimestampType, nullable = true)
    .add("campaignID", IntegerType, nullable = true)
    .add("customer_id", IntegerType, nullable = true)
    .add("status", StringType, nullable = true)

}

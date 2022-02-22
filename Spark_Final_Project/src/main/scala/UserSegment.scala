import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UserSegment {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("UserSegment")
      .master("local[*]")
      .getOrCreate()

    val temp_date = "2021-12-10"

    val userSchema = new StructType()
      .add("customer_id", IntegerType, nullable = true)
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

    val userDF = spark.read
      .option("header", "true").option("delimiter", "\t")
      .schema(userSchema)
      .csv("data/source/users/" + temp_date)

    val promotionDF = spark.read
      .option("header", "true").option("delimiter", "\t")
      .schema(promotionSchema)
      .csv("data/source/promotions/" + temp_date)

    val transactionDF = spark.read
      .option("header", "true").option("delimiter", "\t")
      .schema(transactionSchema)
      .csv("data/source/transactions/" + temp_date)

    userDF.write.mode(SaveMode.Overwrite).parquet("data/datalake/users/" + temp_date)
    promotionDF.write.mode(SaveMode.Overwrite).parquet("data/datalake/promotions/" + temp_date)
    transactionDF.write.mode(SaveMode.Overwrite).parquet("data/datalake/transactions/" + temp_date)

    val userWithAgeDF = userDF.withColumn("age", year(lit(temp_date)) - year(col("birthdate")))
      .drop("birthdate")

  }
}

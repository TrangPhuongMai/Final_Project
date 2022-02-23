import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.bson.Document

object UserSegment {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("UserSegment")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val temp_date = "2021-11-02"

    val output_uri_test = "mongodb://localhost:27017/"

    def getDFfromMongo(df: DataFrame, colname: String, collection: String): DataFrame = {
      val readConfig = ReadConfig(Map("uri" -> output_uri_test, "database" -> "FAcademy", "collection" -> collection))
      val text = df.select(col(colname)).agg(concat_ws(",", collect_list(col(colname)))).first().get(0)
      val pipeline = "{$match: { _id: {$in:[" + text + "]}}}"
      val rdd = MongoSpark.load(spark.sparkContext, readConfig)
      val result = rdd.withPipeline(Seq(Document.parse(pipeline)))
        .toDF()
      return result
    }

    def writeDFtoMongo(df: DataFrame, primcol: String, collect: String) = {
      val writeConfig = WriteConfig(Map("uri" -> output_uri_test, "database" -> "FAcademy", "collection" -> collect, "replaceDocument" -> "false"))
      val df_save = df.withColumnRenamed(primcol, "_id")
      MongoSpark.save(df_save, writeConfig)
    }

    // Create schemas
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

    // Read data from sources
    val trantypeDF = spark.read
      .option("header", "true").option("delimiter", "\t")
      .schema(tranTypeSchema)
      .csv("data/source/mapping/transtype.csv")

    val genderDF = spark.read
      .option("header", "true").option("delimiter", "\t")
      .schema(genderSchema)
      .csv("data/source/mapping/gender.csv")

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

    val campaignDF = spark.read
      .option("header", "true").option("delimiter", "\t")
      .schema(campaignSchema)
      .csv("data/source/configs/campaign.csv")

    // Write data to datalake
    userDF.write.mode(SaveMode.Overwrite).parquet("data/datalake/users/" + temp_date)
    promotionDF.write.mode(SaveMode.Overwrite).parquet("data/datalake/promotions/" + temp_date)
    transactionDF.write.mode(SaveMode.Overwrite).parquet("data/datalake/transactions/" + temp_date)

    // TRANSFORM DATA
    // Get last updated demographic infomation of the day
    val userWindowSpec = Window.partitionBy("userId").orderBy(col("updatedTime").desc)
    val userFinalDF = userDF.withColumn("Rank", row_number().over(userWindowSpec))
      .filter($"Rank" === 1).drop($"Rank")

    val userWithAgeDF = userFinalDF.withColumn("age", year(lit(temp_date)) - year(col("birthdate")))
      .drop("birthdate")
    val dayUserResult = userWithAgeDF.as("user").join(broadcast(genderDF.as("gender")),
      col("user.gender") === col("gender.gender")).drop("gender")
      .withColumnRenamed("genderName", "gender")
      .cache()

    // Filter successful transactions
    val successTransDF = transactionDF.filter(col("transStatus") === 1)
    val transWithTypeNameDF = successTransDF.as("trans").join(broadcast(trantypeDF.as("type")),
      col("trans.transType") === col("type.trantype"))
      .drop("trantype")
      .cache()

    // Get user activities data
    val userActiveDF = transWithTypeNameDF.select("userId").distinct()
      .withColumn("FirstActiveDate", to_date(lit(temp_date)))
      .withColumn("LastActiveDate", to_date(lit(temp_date)))
    val userActivePaymentDF = transWithTypeNameDF.filter(col("transtypename") === "Payment").select("userId").distinct()
      .withColumn("FirstPayDate", to_date(lit(temp_date)))
      .withColumn("LastPayDate", to_date(lit(temp_date)))


    val windowSpec = Window.partitionBy("userId").orderBy(col("transtypename").desc)

    val lastTransactionDF = transWithTypeNameDF.withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .select(col("userId"), col("appId").as("lastPayAppId"), col("transType").as("lastActiveTransactionType"))

    val appPmcIdsDF = transWithTypeNameDF.groupBy("userId").agg(collect_set("appId").as("appIds"),
      collect_set("pmcId").as("pmcIds"))


    val activeJointDF = userActiveDF.as("active").join(userActivePaymentDF.as("payment"),
      col("active.userId") === col("payment.userId"), "left")
      .drop(col("payment.userId"))

    val lastTranJoint = activeJointDF.as("active").join(lastTransactionDF.as("last"),
      col("active.userId") === col("last.userId"))
      .drop(col("last.userId"))

    val dayTransResultDF = lastTranJoint.as("last").join(appPmcIdsDF.as("appPmc"),
      col("last.userId") === col("appPmc.userId"))
      .drop(col("appPmc.userId"))
      .cache()

    //    dayTransResultDF.orderBy(size(col("appIds")).desc,size(col("pmcIds")).desc).show()

    //  Get promotions data
    val promotionInfo = promotionDF.as("promotion").join(broadcast(campaignDF.as("camp")),
      col("promotion.campaignID") === col("camp.campaignID")).drop(col("camp.campaignID"))


    val promotionValidDate = promotionInfo.withColumn("ValidDate", when($"status" === "USED", $"time")
      .when($"campaignType" === 1, $"expireDate")
      .when(($"expireDate".cast("long") - $"time".cast("long")) < $"expireTime", $"expireDate")
      .otherwise(($"time".cast("long") + $"expireTime").cast("timestamp")))
      .drop($"expireDate").drop($"expireTime").drop($"campaignType")

    val PWindowSpec = Window.partitionBy("voucherCode").orderBy(col("time").desc)

    // window filter to avoid user received and use the voucher in the same day.
    val dayPromotionResult = promotionValidDate.withColumn("Rank", row_number().over(PWindowSpec))
      .filter($"Rank" === 1).drop($"Rank").drop($"time")

    //    println(getDFfromMongo(dayUserResult, "userId", "demographic").count())
    //    writeDFtoMongo(dayUserResult, "userId", "demographic")


  }
}

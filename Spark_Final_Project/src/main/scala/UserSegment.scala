import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import ult.Schema._
import ult.MongoDBConnector._

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





    //  Get dayActivity
    val windowlastpayid = Window.partitionBy("userID").orderBy($"transactionTime".desc)
    val dayActivityResultDF = transWithTypeNameDF
      .withColumn("FirstActiveDate", to_date(lit(temp_date)))
      .withColumn("LastActiveDate", to_date(lit(temp_date)))
      .withColumn("FirstPayDate", when($"transtypename" === "Payment", $"FirstActiveDate").otherwise(null))
      .withColumn("LastPayDate", when($"transtypename" === "Payment", $"LastActiveDate").otherwise(null))
      .withColumn("LastPayAppID", when($"transtypename" === "Payment", first($"appId").over(windowlastpayid)).otherwise(null))
      .withColumn("LastActiveTransactionType", first($"trantype").over(windowlastpayid))
      .withColumn("paymentPmcIDs", when($"transtypename" === "Payment", $"pmcId").otherwise(null))
      .groupBy($"userId").agg(first($"FirstActiveDate").as("FirstActiveDate"),
      first($"LastActiveDate").as("LastActiveDate"), collect_set("appId").as("appIds"),  collect_set("paymentPmcIDs").as("pmcIds"),
      first("FirstPayDate", true).as("FirstPayDate"), first("LastPayDate",true).as("LastPayDate")
      , first("LastPayAppID").as("LastPayAppID"), first($"LastActiveTransactionType").as("LastActiveTransactionType")).cache()

    // End dayActivityResultDF


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
      .cache()


    // INSERT DATA TO DATABASE

    // Insert into demographic table
    val inputDemographicDF = dayUserResult.as("day").join(
      getDFfromMongo(spark, dayUserResult, "userId", "Demographic", dbDemographicSchema)
        .select("_id", "updatedTime")
        .as("db"),
      $"day.userId" === $"db._id", "left")
      .withColumn("input", when($"db.updatedTime".isNull, 1)
        .when($"db.updatedTime" < $"day.updatedTime", 1).otherwise(0))
      .filter($"input" === 1)
      .drop($"db.updatedTime").drop($"_id").drop("input")

    writeDFtoMongo(spark, inputDemographicDF, "userId", "Demographic")

    //Insert into activity table
    val firstActiveCondition = when($"db.dbFirstActiveDate".isNull, $"day.FirstActiveDate")
      .when($"db.dbFirstActiveDate" > $"day.FirstActiveDate", $"day.FirstActiveDate")
      .otherwise($"db.dbFirstActiveDate")

    val lastActiveCondition = when($"db.dbLastActiveDate".isNull, $"day.LastActiveDate")
      .when($"db.dbLastActiveDate" < $"day.LastActiveDate", $"day.LastActiveDate")
      .otherwise($"db.dbLastActiveDate")

    val firstPayDateCondition = when($"db.dbFirstPayDate".isNull, $"day.FirstPayDate")
      .when($"day.FirstPayDate".isNull, $"db.dbFirstPayDate")
      .when($"db.dbFirstPayDate" > $"day.FirstPayDate", $"day.FirstPayDate")
      .otherwise($"db.dbFirstPayDate")

    val lastPayDateCondition = when($"db.dbLastPayDate".isNull , $"day.LastPayDate")
      .when($"day.LastPayDate".isNull, $"db.dbLastPayDate")
      .when($"db.dbLastPayDate" < $"day.LastPayDate", $"day.LastPayDate")
      .otherwise($"db.dbLastPayDate")
    // Prioritize day last pay
    val lastPayAppIdCondition = when($"db.dbLastPayDate".isNull, $"day.lastPayAppId")
      .when($"day.LastPayDate".isNull, $"db.dblastPayAppId")
      .when($"db.dbLastPayDate" > $"day.LastPayDate", $"db.dblastPayAppId")
      .otherwise($"day.lastPayAppId")

    val lastActiveTransactionTypeCondition = when($"db.dbLastActiveDate".isNull, $"day.lastActiveTransactionType")
      .when($"db.dbLastActiveDate" > $"day.LastActiveDate", $"db.dblastActiveTransactionType")
      .otherwise($"day.lastActiveTransactionType")

    val appIdsCondition = when($"db.dbappIds".isNull, $"day.appIds")
      .otherwise(array_union($"db.dbappIds", $"day.appIds"))

    val pmcIdsCondition = when($"db.dbpmcIds".isNull, $"day.pmcIds")
      .otherwise(array_union($"db.dbpmcIds", $"day.pmcIds"))


    // Kiểm tra lại thật kĩ phần này
    val dbInputCondition = when($"_id".isNull, 1)
      .when($"LastActiveDate" =!= $"dbLastActiveDate",1)
      .when($"LastPayDate"=!= $"dbLastPayDate", 1)
      .when($"FirstPayDate"=!= $"dbFirstPayDate", 1)
      .when($"FirstActiveDate" =!= $"dbFirstActiveDate",1 )
      .when($"appIds" =!= $"dbappIds", 1)
      .when($"pmcIds" =!=$"dbpmcIds",1 ).otherwise(0)

    val dbActivityDF = getDFfromMongo(spark, dayActivityResultDF, "userId", "Activity", dbActivitySchema)
      .select($"_id", $"FirstActiveDate".as("dbFirstActiveDate"),
        $"FirstPayDate".as("dbFirstPayDate"),
        $"LastActiveDate".as("dbLastActiveDate"),
        $"LastPayDate".as("dbLastPayDate"),
        $"lastActiveTransactionType".as("dblastActiveTransactionType"),
        $"lastPayAppId".as("dblastPayAppId"),
        $"pmcIds".as("dbpmcIds"),
        $"appIds".as("dbappIds"))

    val inputActivity = dayActivityResultDF.as("day").join(dbActivityDF.as("db"),
      $"userId" === $"_id", "left").withColumn("pmcIds", pmcIdsCondition)
      .withColumn("appIds", appIdsCondition)
      .withColumn("lastActiveTransactionType", lastActiveTransactionTypeCondition)
      .withColumn("lastPayAppId", lastPayAppIdCondition)
      .withColumn("LastPayDate", lastPayDateCondition)
      .withColumn("FirstPayDate", firstPayDateCondition)
      .withColumn("LastActiveDate", lastActiveCondition)
      .withColumn("FirstActiveDate", firstActiveCondition)
      .withColumn("Input", dbInputCondition)
      .filter($"Input" === 1)
      .drop("_id", "dbFirstActiveDate", "dbFirstPayDate", "dbLastActiveDate", "dbLastPayDate", "dblastActiveTransactionType", "dblastPayAppId", "dbpmcIds", "dbappIds", "Input")
    writeDFtoMongo(spark, inputActivity, "userId", "Activity")


    // Insert into promotion table
    val profromdb = getDFfromMongo(spark, dayPromotionResult, "voucherCode", "Promotion", db_promo_schema, true)
      .select($"_id", $"ValidDate".as("OldValidDate"), $"status".as("OldStatus"))

    val promotionProcessing = dayPromotionResult.as("promotion").join(profromdb.as("dbdata"), $"promotion.voucherCode" === $"dbdata._id", "leftouter")
    val instantinput = promotionProcessing.withColumn("Input", when($"OldValidDate".isNull, 1)
      .when($"status" === "USED" and $"OldStatus" === "GIVEN", 1).otherwise(0)).filter($"Input" === 1).drop($"Input")
      .drop($"_id").drop("OldValidDate").drop("OldStatus")

    writeDFtoMongo(spark, instantinput, "voucherCode", "Promotion")

    spark.close()
  }
}

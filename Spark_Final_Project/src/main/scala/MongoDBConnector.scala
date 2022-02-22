import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, datediff, min, to_date}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
object MongoDBConnector {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MongoDBConnector")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._



    val output_uri_test = "mongodb://localhost:27017/FPTAcademy"


    // Test mongoreadfile
    val Ecomschema2 = StructType(Array(
      StructField("InvoiceNo", IntegerType, false),
      StructField("StockCode", StringType, false),
      StructField("Description", StringType, true),
      StructField("Quantity", IntegerType, true),
      StructField("InvoiceDate", DateType, true),
      StructField("UnitPrice", DoubleType,  true), // MongoDB ko có floattype nên phải để là double
      StructField("CustomerID", IntegerType, true ),
      StructField("Country", StringType, true)
    ))
    val ReadDF = spark.read
      .format("com.mongodb.spark.sql.DefaultSource").schema(Ecomschema2)
      .option("uri", output_uri_test)
      .option("database", "FPTAcademy")
      .option("collection", "Transaction")
      .load()

    ReadDF.show()


    // Test mongodb write
    val campaign_schema = new StructType()
      .add("voucherCode",StringType)
      .add("Status", StringType)
      .add("expireDate", StringType)
      .add("expireTime", IntegerType )
      .add("Time", StringType)


    val outerschema = new StructType()
      .add("user_id", StringType)
      .add("age", IntegerType)
      .add("gender", IntegerType)
      .add("ProfileLevel", IntegerType)
      .add("LastUpdated", StringType)
      .add("FirstPayDate", StringType)
      .add("FirstActiveDate", StringType)
      .add("LastPayDate", StringType)
      .add("LastActiveDate", StringType)
      .add("LastTransactionType", IntegerType)
      .add("LastPayAppID", IntegerType)
      .add("Campaign", MapType(StringType, campaign_schema))

    val datatest = Seq(Row("123",13,1,1,"2021-11-06 18:29:19.104","2021-11-06 18:29:19.104",
      "2021-11-06 18:29:19.104","2021-11-06 18:38:17.838", "2021-11-06 18:38:17.838",
      1,1, Map("1000" -> Row("1000-5","GIVEN","2021-11-01 13:31:04.561",10,"2022-01-01 00:00:00"))))


    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(datatest),outerschema)

    df.write.format("com.mongodb.spark.sql.DefaultSource")
          .mode("append")
          .option("uri", output_uri_test)
          .option("database", "FPTAcademy")
          .option("collection", "Datatest").save()


    spark.stop()
  }
}

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, datediff, min, to_date}
import com.mongodb.spark.MongoSpark
import org.bson.{BsonDocument, Document}
object MongoDBConnector {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MongoDBConnector")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    import com.mongodb.spark.config._


    // Set up to read from mongoDB
    val output_uri_test = "mongodb://localhost:27017/"

    def getDFfromMongo(df: DataFrame, colname: String, collection: String): DataFrame = {
      val readConfig = ReadConfig(Map("uri" -> output_uri_test,"database" -> "FAcademy", "collection" ->collection))
      val text = df.select(col(colname)).agg(concat_ws(",",collect_list(col(colname)))).first().get(0)
      val pipeline = "{$match: {"+ colname +" : {$in:[" + text + "]}}}"
      val rdd = MongoSpark.load(spark.sparkContext, readConfig)
      val result = rdd.withPipeline(Seq(Document.parse(pipeline)))
        .toDF()
      return result
    }

    def writeDFtoMongo(df: DataFrame, primcol: String, collect: String) ={
      val writeConfig = WriteConfig(Map("uri" -> output_uri_test,"database" -> "FAcademy", "collection" ->collect,"replaceDocument" -> "false"))
      val df_save = df.withColumnRenamed(primcol, "_id")
      MongoSpark.save(df_save,writeConfig)
    }

    // save this 


    spark.stop()
  }
}

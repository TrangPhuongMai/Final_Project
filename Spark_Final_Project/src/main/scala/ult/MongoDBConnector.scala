package ult

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, row_number}
import org.apache.spark.sql.types.StructType
import org.bson.Document

object MongoDBConnector {
  val output_uri_test = "mongodb://localhost:27017/"

  def getDFfromMongo(spark: SparkSession, df: DataFrame, colname: String, collection: String, schema: StructType, isStringType: Boolean = false): DataFrame = {
    val readConfig = ReadConfig(Map("uri" -> output_uri_test, "database" -> "FAcademy", "collection" -> collection))
    val pipeline: String = if (isStringType) {
      val text = df.select(col(colname)).agg(concat_ws("""","""", collect_list(col(colname)))).first().get(0)
      "{$match: { _id : {$in:[\"" + text + "\"]}}}"
    }
    else {
      val text = df.select(col(colname)).agg(concat_ws(",", collect_list(col(colname)))).first().get(0)

      "{$match: { _id : {$in:[" + text + "]}}}"
    }

    val rdd = MongoSpark.load(spark.sparkContext, readConfig)
    val result = rdd.withPipeline(Seq(Document.parse(pipeline))).toDF(schema)
    return result
  }

  def writeDFtoMongo(spark: SparkSession, df: DataFrame, primcol: String, collect: String) = {
    val writeConfig = WriteConfig(Map("uri" -> output_uri_test, "database" -> "FAcademy", "collection" -> collect, "replaceDocument" -> "false"))
    val df_save = df.withColumnRenamed(primcol, "_id")
    MongoSpark.save(df_save, writeConfig)
  }

  def getDFbyBatch(spark: SparkSession, df:DataFrame, colName: String, collection: String,  schemain: StructType, isStringtype: Boolean = false, batch_size: Int = 1000 ): DataFrame ={
    val w = Window.orderBy(colName)
    var lowerbound = 1
    var upperbound = batch_size
    val wantedcol = df.select(colName).withColumn("idx", row_number().over(w)).cache()
    var dummydf = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], schemain)
    val totalrow = wantedcol.count()
    while( lowerbound <= totalrow){
      val takefrom = wantedcol.filter(col("idx").between(lowerbound,upperbound)).select(colName)
      dummydf = dummydf.unionAll(getDFfromMongo(spark, takefrom,colName, collection, schemain, isStringtype))
      lowerbound += batch_size
      upperbound += batch_size
    }

    return dummydf

  }
}

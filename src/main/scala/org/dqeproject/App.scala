package org.dqeproject

import com.amazon.deequ.analyzers.{Completeness, HdfsStateProvider}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val spark = SparkSession.builder().master("local").config("spark.sql.shuffle.partitions",5).getOrCreate()
    val pathStateStore_last = "C:\\Users\\mkabariya\\OneDrive - Tata CLiQ\\Desktop\\Test_Location_Store_1\\store_file\\01\\"
    val pathStateStore_current = "C:\\Users\\mkabariya\\OneDrive - Tata CLiQ\\Desktop\\Test_Location_Store\\store_file\\02\\"

    val df_last = spark.read.parquet("C:\\Users\\mkabariya\\IdeaProjects\\DQProfiling\\src\\test\\resources\\data\\part-00031-a618a938-81ea-43d2-8520-5e6c78582193.c000.snappy.parquet")
    val df_current = spark.read.parquet("C:\\Users\\mkabariya\\IdeaProjects\\DQProfiling\\src\\test\\resources\\data\\part-00055-a618a938-81ea-43d2-8520-5e6c78582193.c000.snappy.parquet")
    //    df_current.printSchema()
    val completenessColumns = Seq("pk","p_calculated", "p_code", "p_currency", "p_deliveryaddress","p_deliverystatus")
    val countDistinctColumns = Seq("pk","p_calculated", "p_code", "p_currency", "p_deliveryaddress","p_deliverystatus")


    val schema = StructType(List(
      StructField("pk", LongType, true),
      StructField("p_calculated", IntegerType, true),
      StructField("p_code", StringType, true),
      StructField("p_currency", LongType, true),
      StructField("p_deliveryaddress", LongType, true),
      StructField("p_deliverystatus",LongType,true)
    ))
    val complianceColumns = Seq("p_currency_greater_then_zero~p_currency>200")
    val patternMatchColumns = Seq("operationname~UPDATE")
    val correlationColumns = Seq("p_deliverycost~p_paymentcost")
    val mapdata = Map("Completeness"->completenessColumns,"CountDistinct"->countDistinctColumns,"Compliance"->complianceColumns)
    val newMapData = Map("Completeness"->completenessColumns,"CountDistinct"->countDistinctColumns)
    val mapDataPatternMatch = Map("PATTERNMATCH" -> patternMatchColumns,"CORRELATION" -> correlationColumns)
    val mapDataCompliance = Map("MUTUALINFORMATION" -> correlationColumns)
    //    val mapSchema = scala.collection.immutable.Map("coltype" -> "TimestampType", "colname" -> "committimestamp","colnull" -> "false")
    //    val mapSchema1 = scala.collection.immutable.Map("coltype" -> "StringType", "colname" -> "operationname","colnull" -> "true")
    //    val seqData = Seq(mapSchema,mapSchema1)
    //    val testSchemaBuild = testSchema(spark,seqData,df_current)
    df_last.show(100)
    getmetricsComputed(spark,pathStateStore_last,df_last,mapDataCompliance)
    //    getmetricsComputed(spark,pathStateStore_current,df_current,mapdata)
    //    getmetricsAggregated(spark,"C:\\Users\\mkabariya\\OneDrive - Tata CLiQ\\Desktop\\Test_Location_Store\\store_file\\01\\"
    //      ,"C:\\Users\\mkabariya\\OneDrive - Tata CLiQ\\Desktop\\Test_Location_Store\\store_file\\02\\",mapdata,schema)

  }

  /***
   *
   * @param spark
   * @param pathStateStore
   * @param data
   * @param buildAanalysisParams
   */
  def getmetricsComputed(spark:SparkSession, pathStateStore:String,data:DataFrame,buildAanalysisParams: scala.collection.Map[String,Seq[String]]) ={

    val analysisBuilder = new AnalysisBuilder()
    val analysis = analysisBuilder.buildAnalysis(buildAanalysisParams)
    val stateStoreDailyData = HdfsStateProvider(session=spark,locationPrefix = pathStateStore)
    println(data.count())
    println(stateStoreDailyData.locationPrefix)

    val metricsForData = AnalysisRunner.run(
      data = data,
      analysis = analysis,
      saveStatesWith = Some(stateStoreDailyData))

    val dataFramemetrics = successMetricsAsDataFrame(spark, metricsForData)
    dataFramemetrics.show(400)
    dataFramemetrics.printSchema()
    dataFramemetrics
  }

  /***
   * @note
   * This function is meant for adding new states to aggregate statestore in case if there is any update in analysis builder(ex. New metrics
   * added to the analysis or new column added to the table)
   * @param spark : Spark session
   * @param aggStateStore : Path to Aggregate State Store
   * @param data : Data in which new changes are present(ex. new column added or new metrics added in Build params)
   * @param newBuildAanalysisParams : Newly added analysis build params in config file
   */
  def getNewmetricsAddedInAggStateStore(spark:SparkSession,aggStateStore:String,data:DataFrame,newBuildAanalysisParams:scala.collection.Map[String,Seq[String]])={
    val analysisBuilder = new AnalysisBuilder()
    val stateStoreDailyData = HdfsStateProvider(session = spark, locationPrefix = aggStateStore)
    val analysis = analysisBuilder.buildAnalysis(newBuildAanalysisParams)
    val metricsForData = AnalysisRunner.run(
      data = data,
      analysis = analysis,
      saveStatesWith = Some(stateStoreDailyData))

    val dataFrameMetrics = successMetricsAsDataFrame(spark, metricsForData)
    dataFrameMetrics.show(400)
    dataFrameMetrics
  }

  /***
   *
   * @param spark
   * @param latestStateStores
   * @param aggStateStore
   * @param buildAanalysisParams
   * @param schema
   */
  def getmetricsAggregated(spark:SparkSession, latestStateStores:String,aggStateStore:String,buildAanalysisParams:scala.collection.Map[String,Seq[String]],schema :StructType)={
    val analysisBuilder = new AnalysisBuilder()

    val analysis = analysisBuilder.buildAnalysis(buildAanalysisParams)
    //    var pathForStateStores = Seq.empty[HdfsStateProvider]
    //    for (pathForSS<- pathStateStores){
    //      pathForStateStores = pathForStateStores :+ HdfsStateProvider(spark,pathForSS)
    //    }
    val pathForStateStores = Seq(HdfsStateProvider(spark,latestStateStores),HdfsStateProvider(spark,aggStateStore))
    val metricsForData = AnalysisRunner.runOnAggregatedStates(schema,analysis,pathForStateStores)

    val dataFramemetrics = successMetricsAsDataFrame(spark, metricsForData)
    dataFramemetrics.show(400)
    dataFramemetrics
  }

  def testFunction()={
    "test_data"
  }

  def testSchema(spark:SparkSession,schema: Seq[scala.collection.immutable.Map[String,String]],data:DataFrame) ={
    val schemaBuilder = new SchemaBuilder()
    val buildSchema = schemaBuilder.schemaBuild(schema)
    print(data.schema)
    buildSchema.foreach(println)
    "test_success"
  }

}

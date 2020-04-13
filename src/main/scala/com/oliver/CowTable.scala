package com.oliver

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.io.Directory


case class Album(albumId: Long, title: String, tracks: Array[String], updateDate: Long)


/**
  * This is a self contained example.
  *
  * @author oliver mascarenhas
  */
object App {
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val basePath = "/tmp/store"

  def dateToLong(dateString: String): Long = LocalDate.parse(dateString, formatter).toEpochDay

  private val INITIAL_ALBUM_DATA = Seq(
    Album(800, "6 String Theory", Array("Lay it down", "Am I Wrong", "68"), dateToLong("2019-12-01")),
    Album(801, "Hail to the Thief", Array("2+2=5", "Backdrifts"), dateToLong("2019-12-01")),
    Album(801, "Hail to the Thief", Array("2+2=5", "Backdrifts", "Go to sleep"), dateToLong("2019-12-03"))
  )

  private val UPSERT_ALBUM_DATA = Seq(
    Album(800, "6 String Theory - Special", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-03")),
    Album(802, "Best Of Jazz Blues", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-04")),
    Album(803, "Birth of Cool", Array("Move", "Jeru", "Moon Dreams"), dateToLong("2020-02-03"))
  )


  def main(args: Array[String]) {

    clearDirectory()

    val spark: SparkSession = SparkSession.builder().appName("hudi-datalake")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .getOrCreate()

    import spark.implicits._
    val tableName = "Album"
    upsert(INITIAL_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate")
    snapshotQuery(spark, tableName)

    // Sleep for 5 seconds, so that we get a different commit time
    Thread.sleep(5000)
    upsert(UPSERT_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate")
    snapshotQuery(spark, tableName)

    incrementalQuery(spark, basePath, tableName)

    deleteQuery(spark, basePath, tableName)

  }

  private def incrementalQuery(spark: SparkSession, basePath: String, tableName: String): Unit = {
    spark.read
      .format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20200412183510")
      .load(s"$basePath/$tableName")
      .show()

  }

  private def deleteQuery(spark: SparkSession, basePath: String, tableName: String): Unit = {
    val deleteKeys = Seq(
      Album(803, "", null, 0l),
      Album(802, "", null, 0l)
    )

    import spark.implicits._

    val df = deleteKeys.toDF()

    df.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "albumId")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append) // Only Append Mode is supported for Delete.
      .save(s"$basePath/$tableName/")

    spark.read.format("hudi").load(s"$basePath/$tableName/*").show()
  }

  private def clearDirectory(): Unit = {
    val directory = new Directory(new File(basePath))
    directory.deleteRecursively()
  }

  private def snapshotQuery(spark: SparkSession, tableName: String): Unit = {
    spark.read.format("hudi").load(s"$basePath/$tableName/*").show()
  }

  private def upsert(albumDf: DataFrame, tableName: String, key: String, combineKey: String): Unit = {
    albumDf.write
      .format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, key)
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, combineKey)
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save(s"$basePath/$tableName/")
  }
}

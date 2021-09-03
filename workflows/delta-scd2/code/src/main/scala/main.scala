import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

import graph._

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_CustomerDimOld: Source            = CustomerDimOld(spark)
    val df_FixDates:       SchemaTransformer = FixDates(spark, df_CustomerDimOld)
    OverwriteDelta(spark, df_FixDates)
    val df_CustomerEdits: Source            = CustomerEdits(spark)
    val df_AddDates:      SchemaTransformer = AddDates(spark, df_CustomerEdits)
    DeltaSCD2Merge(spark, df_AddDates)
    val df_ReadMerged: Source = ReadMerged(spark)
    Observe(spark, df_ReadMerged)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark = SparkSession
      .builder()
      .appName("id")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()

    UDFs.registerUDFs(spark)
    UDAFs.registerUDAFs(spark)

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}

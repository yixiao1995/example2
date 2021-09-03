import org.apache.spark.sql.types._
import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

import graph._

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_Customer: Source = Customer(spark)

    val df_Orders:            Source    = Orders(spark)
    val df_PerCustomer:       Join      = PerCustomer(spark,       df_Customer, df_Orders)
    val df_TotalByCustomer:   Aggregate = TotalByCustomer(spark,   df_PerCustomer)
    val df_Cleanup:           Reformat  = Cleanup(spark,           df_TotalByCustomer)
    val df_SortBiggestOrders: OrderBy   = SortBiggestOrders(spark, df_Cleanup)
    WriteReport(spark, df_SortBiggestOrders)

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

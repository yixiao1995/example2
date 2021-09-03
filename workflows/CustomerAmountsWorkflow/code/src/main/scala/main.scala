import org.apache.spark.sql.types._
import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

import graph._

@Visual(mode = "batch")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_CustomersDatasetInput: Source = CustomersDatasetInput(spark)

    val df_OrdersDatasetInput: Source    = OrdersDatasetInput(spark)
    val df_JoinComponent:      Join      = JoinComponent(spark,      df_OrdersDatasetInput, df_CustomersDatasetInput)
    val df_PrepareComponent:   Reformat  = PrepareComponent(spark,   df_JoinComponent)
    val df_AggregateComponent: Aggregate = AggregateComponent(spark, df_PrepareComponent)
    CustomerOrdersDatasetOutput(spark, df_AggregateComponent)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark = SparkSession
      .builder()
      .appName("CustomerAmounts")
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

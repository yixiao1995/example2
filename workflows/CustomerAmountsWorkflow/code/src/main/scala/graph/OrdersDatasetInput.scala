package graph

import org.apache.spark.sql.types._
import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "OrdersDatasetInput", label = "OrdersDatasetInput", x = 7, y = 42, phase = 0)
object OrdersDatasetInput {

  @UsesDataset(id = "4266", version = 2)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val out = Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",       IntegerType, false),
            StructField("customer_id",    IntegerType, false),
            StructField("order_status",   StringType,  false),
            StructField("order_category", StringType,  false),
            StructField("order_date",     StringType,  false),
            StructField("amount",         DoubleType,  false)
          )
        )
        spark.read
          .format("csv")
          .option("sep", ",")
          .schema(schemaArg)
          .load("dbfs:/Prophecy/464437275@qq.com/OrdersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}

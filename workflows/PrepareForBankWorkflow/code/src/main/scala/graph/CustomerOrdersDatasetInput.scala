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

@Visual(id = "CustomerOrdersDatasetInput", label = "CustomerOrdersDatasetInput", x = 7, y = 98, phase = 0)
object CustomerOrdersDatasetInput {

  @UsesDataset(id = "4267", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val out = Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",            IntegerType, false),
            StructField("orders",              IntegerType, false),
            StructField("amount",              DoubleType,  false),
            StructField("customer_id",         IntegerType, false),
            StructField("first_name",          StringType,  false),
            StructField("last_name",           StringType,  false),
            StructField("phone",               StringType,  false),
            StructField("email",               StringType,  false),
            StructField("country_code",        StringType,  false),
            StructField("account_length_days", IntegerType, false),
            StructField("account_flags",       StringType,  false)
          )
        )
        spark.read
          .format("csv")
          .option("sep", ",")
          .schema(schemaArg)
          .load("dbfs:/Prophecy/464437275@qq.com/CustomerOrdersDatasetOutput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}

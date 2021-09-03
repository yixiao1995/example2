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

@Visual(id = "Customer", label = "Customer", x = 88, y = 179, phase = 0)
object Customer {

  @UsesDataset(id = "4275", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val out = Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",       IntegerType,   true),
            StructField("first_name",        StringType,    true),
            StructField("last_name",         StringType,    true),
            StructField("phone",             StringType,    true),
            StructField("email",             StringType,    true),
            StructField("country_code",      StringType,    true),
            StructField("account_open_date", TimestampType, true),
            StructField("account_flags",     StringType,    true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/Prophecy/464437275@qq.com/CustomersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}

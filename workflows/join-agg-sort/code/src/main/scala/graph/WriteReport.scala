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

@Visual(id = "WriteReport", label = "WriteReport", x = 1352, y = 194, phase = 0)
object WriteReport {

  @UsesDataset(id = "4276", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(StructField("name",        StringType,  false),
                StructField("amount",      LongType,    false),
                StructField("customer_id", IntegerType, false)
          )
        )
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("overwrite")
          .save("dbfs:/Prophecy/464437275@qq.com/CustomerTotals")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}

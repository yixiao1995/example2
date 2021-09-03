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

@Visual(id = "ReportDatasetOutput", label = "ReportDatasetOutput", x = 373, y = 154, phase = 0)
object ReportDatasetOutput {

  @UsesDataset(id = "4269", version = 1)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("id",           IntegerType, false),
            StructField("report_title", StringType,  false),
            StructField("customers",    IntegerType, false),
            StructField("amount_total", DoubleType,  false),
            StructField("orders_total", IntegerType, false)
          )
        )
        in.write
          .format("csv")
          .option("sep", ",")
          .mode("overwrite")
          .save("dbfs:/Prophecy/464437275@qq.com/ReportDatasetOutput.csv")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}

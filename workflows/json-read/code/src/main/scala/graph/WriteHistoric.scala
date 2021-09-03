package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "WriteHistoric", label = "WriteHistoric", x = 584, y = 123, phase = 0)
object WriteHistoric {

  @UsesDataset(id = "4278", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("description", StringType, false),
            StructField("date",        StringType, false),
            StructField("category1",   StringType, false),
            StructField("category2",   StringType, false)
          )
        )
        in.write
          .format("delta")
          .option("fileFormat", "parquet")
          .mode("overwrite")
          .saveAsTable("default.prophecy_historic_event")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}

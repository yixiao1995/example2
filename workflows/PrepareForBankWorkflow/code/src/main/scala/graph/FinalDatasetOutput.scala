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

@Visual(id = "FinalDatasetOutput", label = "FinalDatasetOutput", x = 251, y = 42, phase = 0)
object FinalDatasetOutput {

  @UsesDataset(id = "4268", version = 1)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("id",                      IntegerType, false),
            StructField("full_name",               StringType,  false),
            StructField("phone",                   IntegerType, false),
            StructField("phone_area_code",         IntegerType, false),
            StructField("email",                   StringType,  false),
            StructField("email_provider",          StringType,  false),
            StructField("orders",                  IntegerType, false),
            StructField("amount",                  DoubleType,  false),
            StructField("delinquent_last_90_days", StringType,  false)
          )
        )
        in.write
          .format("csv")
          .option("sep", ",")
          .mode("overwrite")
          .save("dbfs:/Prophecy/464437275@qq.com/FinalDatasetOutput.csv")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}

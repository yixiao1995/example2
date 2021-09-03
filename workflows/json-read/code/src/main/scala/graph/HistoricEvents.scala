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

@Visual(id = "HistoricEvents", label = "HistoricEvents", x = 70, y = 122, phase = 0)
object HistoricEvents {

  @UsesDataset(id = "4279", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val out = Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField(
              "result",
              StructType(
                Array(
                  StructField("count", StringType, true),
                  StructField(
                    "events",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("category1",   StringType, true),
                          StructField("category2",   StringType, true),
                          StructField("date",        StringType, true),
                          StructField("description", StringType, true),
                          StructField("granularity", StringType, true),
                          StructField("lang",        StringType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            )
          )
        )
        spark.read
          .format("json")
          .option("multiLine", true)
          .schema(schemaArg)
          .load("dbfs:/Prophecy/464437275@qq.com/old_events_data.json")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}

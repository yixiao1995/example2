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

@Visual(id = "Observe", label = "Observe", x = 352, y = 607, phase = 3)
object Observe {

  @UsesDataset(id = "4271", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(Array(StructField("a", StringType, true)))
        in.count()
        val data = Seq(Row("1"))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schemaArg)
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}

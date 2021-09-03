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

@Visual(id = "Cleanup", label = "Cleanup", x = 850, y = 193, phase = 0)
object Cleanup {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      concat(col("first_name"), lit(" "), col("last_name")).as("name"),
      ceil(col("amount")).as("amount"),
      col("customer_id")
    )

    out

  }

}

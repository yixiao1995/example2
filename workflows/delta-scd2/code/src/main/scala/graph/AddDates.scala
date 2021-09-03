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

@Visual(id = "AddDates", label = "AddDates", x = 343, y = 362, phase = 2)
object AddDates {

  def apply(spark: SparkSession, in: DataFrame): SchemaTransformer = {
    import spark.implicits._

    val out =
      in.withColumn("eff_start_date", current_date()).withColumn("eff_end_date", lit("9999-12-31").cast(DateType))

    out

  }

}

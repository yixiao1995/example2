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

@Visual(id = "PrepareComponent", label = "PrepareComponent", x = 251, y = 98, phase = 0)
object PrepareComponent {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      datediff(current_date(), col("account_open_date")).as("account_length_days"),
      col("order_id"),
      col("customer_id"),
      col("amount"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_flags")
    )

    out

  }

}

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

@Visual(id = "PerCustomer", label = "PerCustomer", x = 369, y = 192, phase = 0)
object PerCustomer {

  def apply(spark: SparkSession, left: DataFrame, right: DataFrame): Join = {
    import spark.implicits._

    val leftAlias  = left.as("left")
    val rightAlias = right.as("right")
    val dfJoin     = leftAlias.join(rightAlias, col("left.customer_id") === col("right.customer_id"), "inner")

    val out = dfJoin.select(
      col("left.first_name").as("first_name"),
      col("left.last_name").as("last_name"),
      col("right.amount").as("amount"),
      col("left.customer_id").as("customer_id")
    )

    out

  }

}

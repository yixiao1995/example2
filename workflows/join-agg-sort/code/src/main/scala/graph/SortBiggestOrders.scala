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

@Visual(id = "SortBiggestOrders", label = "SortBiggestOrders", x = 1104, y = 194, phase = 0)
object SortBiggestOrders {

  def apply(spark: SparkSession, in: DataFrame): OrderBy = {
    import spark.implicits._

    val out = in.orderBy(col("amount").desc)

    out

  }

}

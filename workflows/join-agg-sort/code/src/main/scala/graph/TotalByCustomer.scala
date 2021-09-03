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

@Visual(id = "TotalByCustomer", label = "TotalByCustomer", x = 601, y = 192, phase = 0)
object TotalByCustomer {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(col("customer_id").as("customer_id"))
    val out = dfGroupBy.agg(max(col("first_name")).as("first_name"),
                            max(col("last_name")).as("last_name"),
                            sum(col("amount")).as("amount")
    )

    out

  }

}

package batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleEstimationBatch {

  implicit class SimpleEstimationBatch(df: DataFrame) {

    /**
      * Simple estimation method that calculates an expansion weight for a given time period, given that
      * some records contain missing target values and all records are otherwise in the same group.
      *
      * @param timeColumn String - Column name representing timestamp.
      * @param targetColumn String - Column name represting column to estimate.
      * @return DataFrame
      */
    def simpleEstimate(timeColumn: String, targetColumn: String): DataFrame = {

      // Persist output column order for clarity.
      val order = List(timeColumn, "reporting_unit", targetColumn, "weight",
                       targetColumn + "_adjusted", targetColumn + "_estimated_total")

      // Count the number of records with non-null values for the target column, per time period.
      val sampleCount = df.where(col(targetColumn).isNotNull)
                          .groupBy(timeColumn)
                          .count()
                          .withColumnRenamed("count", "sample_count")

      // Count the number of records per time period.
      val populationCount = df.groupBy(timeColumn)
                              .count()
                              .withColumnRenamed("count", "population_count")

      // Join the counts, create weight as ratio of population vs sample, multiply weight by target.
      val dfAppliedWeights = df.join(sampleCount, Seq(timeColumn))
                                   .join(populationCount, Seq(timeColumn))
                                   .withColumn("weight", col("population_count") / col("sample_count"))
                                   .withColumn(targetColumn + "_adjusted", col(targetColumn) * col("weight"))

      // Perform aggregation.
      val dfEstimatedTotal = dfAppliedWeights.groupBy(timeColumn)
                                             .sum(targetColumn)
                                             .withColumnRenamed("sum(" + targetColumn + ")", targetColumn + "_estimated_total")

      // Join aggregation and select columns.
      dfAppliedWeights.join(dfEstimatedTotal, Seq(timeColumn))
                      .select(order.head, order.tail: _*)
    }
  }
}

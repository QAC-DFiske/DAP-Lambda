package batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleEstimationBatch {

  implicit class SimpleEstimationBatch(df: DataFrame) {

    /**
      * Simple estimation method that calculates an expansion estimate for the total of a target column, given that
      * some records contain missing values.
      *
      * @param timeColumn String - Column name representing timestamp.
      * @param targetColumn String - Column name represting column to estimate.
      * @return DataFrame
      */
    def simpleEstimate(timeColumn: String, targetColumn: String): DataFrame = {

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
      val dfAdjusted = df.join(sampleCount, Seq(timeColumn))
                         .join(populationCount, Seq(timeColumn))
                         .withColumn("weight", col("population_count") / col("sample_count"))
                         .withColumn(targetColumn + "_adjusted", col(targetColumn) * col("weight"))

      // Sum adjusted target to create estimate for total target in time period.
      val dfWithSum = dfAdjusted.groupBy(timeColumn)
                                .sum(targetColumn + "_adjusted")
                                .withColumnRenamed("sum(" + targetColumn + "_adjusted)", targetColumn + "_total_estimate")

      // Persist output column order for clarity.
      val order = List(timeColumn, targetColumn, "weight", targetColumn + "_adjusted", targetColumn + "_total_estimate")

      // Join total estimate, trim and order columns.
      dfWithSum.join(dfAdjusted, Seq(timeColumn)).select(order.head, order.tail: _*)
    }
  }
}

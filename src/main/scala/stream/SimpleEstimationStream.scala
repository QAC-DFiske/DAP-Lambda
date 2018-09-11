package stream

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SimpleEstimationStream {

  implicit class SimpleEstimationStream(df: DataFrame) {

    /**
      * Simple estimation method that joins an expansion weight for a given time period, using a lookup table,
      * given that some records contain missing target values and all records are otherwise in the same group.
      *
      * @param timeColumn String - Column name representing timestamp.
      * @param targetColumn String - Column name representing column to estimate.
      * @param lookupDF DataFrame - Data containing weights to use.
      * @return DataFrame
      */
    def streamSimpleEstimate(timeColumn: String, targetColumn: String,
                             lookupDF: DataFrame, isAggregated: Boolean = false): DataFrame = {

      // Persist output column order for clarity.
      val order = List(timeColumn, "reporting_unit", targetColumn, "weight", targetColumn + "_adjusted")

      val lookupColumnRenamed = lookupDF.withColumnRenamed(timeColumn, timeColumn + "_temp")

      // Join with lookup table, fill nulls with most recent value, multiply target by new weight.
      val joinedWithLookup =  lookupColumnRenamed.join(df, lookupColumnRenamed(timeColumn + "_temp") === df(timeColumn), "right")
                                                 .na.fill(lookupColumnRenamed.orderBy(desc(timeColumn)).select("weight").first().getDouble(0),
                                                          Seq("weight"))
                                                 .withColumn(targetColumn + "_adjusted", col(targetColumn) * col("weight"))
                                                 .select(order.head, order.tail: _*)

      // If isAggregated - perform aggregation, otherwise return joinedWithLookup.
      if (isAggregated) {joinedWithLookup.groupBy(timeColumn).sum(targetColumn)}
      else joinedWithLookup
    }
  }
}


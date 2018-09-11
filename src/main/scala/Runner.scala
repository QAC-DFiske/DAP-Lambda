import batch.SimpleEstimationBatch.SimpleEstimationBatch
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FileSystem, FileUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import stream.SimpleEstimationStream.SimpleEstimationStream

import scala.util.Try

object Config {

  // Create Spark session.
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("lambda").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  // Set config constants.
  val TIME_COLUMN = "time"
  val IDENTIFIER_COLUMN = "reporting_unit"
  val TARGET_COLUMN = "turnover"
  val WEIGHT_COLUMN = "weight"

  // Set IO method constants.
  val INGESTION_PATH = reflect.io.Path(".resources/ingestion")
  val RAW_PATH = reflect.io.Path("./resources/storage/raw")
  val PROCESSED_PATH = reflect.io.Path("./resources/storage/processed")
  val HADOOP_CONFIG = new Configuration()
  val HDFS: FileSystem = FileSystem.get(HADOOP_CONFIG)
}

object BatchRunner {

  import Config._

  /** Coalesce all PART files into single file and rename.
    *
    * @param srcPath String - Directory containing PART files.
    * @param dstPath String - Path to save new file (implicitly set name here)
    */
  def merge(srcPath: String, dstPath: String): Unit = {
    FileUtil.copyMerge(HDFS, new fs.Path(srcPath), HDFS, new fs.Path(dstPath), false, HADOOP_CONFIG, null)
  }

  /** Copy file to new directory.
    *
    * @param srcPath String - Path to file for copying.
    * @param dstPath String - Destination path.
    */
  def copy(srcPath: String, dstPath: String): Unit = {
    FileUtil.copy(new java.io.File(srcPath), HDFS, new fs.Path(dstPath), false, HADOOP_CONFIG)
  }

  def main(args: Array[String]): Unit = {

    // Delete created directory if exists.
    Try(PROCESSED_PATH.deleteRecursively)

    // Read batch data into DataFrame.
    val batchInput = spark.read.json(RAW_PATH + "/batchRaw.json")
    println("Batch input:")
    batchInput.show()

    // Perform SML-like function on DataFrame.
    val batchProcessed = batchInput.simpleEstimate(TIME_COLUMN, TARGET_COLUMN)
    println("Batch processed:")
    batchProcessed.show()

    // Write to temporary directory, coalesce part files into permanent directory, delete temporary directory.
    batchProcessed.write.json(PROCESSED_PATH + "/batchProcessedTemp.json")
    merge(PROCESSED_PATH + "/batchProcessedTemp.json", PROCESSED_PATH.path + "/batchProcessed.json")
    Try(reflect.io.Path(PROCESSED_PATH + "/batchProcessedTemp.json").deleteRecursively)
  }
}

object StreamRunner {

  import Config._

  def main(args: Array[String]): Unit = {

    // Read processed data for join later.
    val processedAggregate = spark.read.json(PROCESSED_PATH + "/batchProcessed.json").select(TIME_COLUMN, WEIGHT_COLUMN)

    // Define input schema.
    val streamSchema = StructType(Seq(StructField("time", StringType, nullable = false),
                                      StructField("reporting_unit", StringType, nullable = false),
                                      StructField("turnover", DoubleType, nullable = true)))

    // Read data from JSON file, apply schema.
    val inputStream = spark.readStream.schema(streamSchema).json("./resources/ingestion")

    // Apply estimation method.
    val transformStream = inputStream.streamSimpleEstimate(TIME_COLUMN, TARGET_COLUMN, processedAggregate)

//    // Apply estimation method with aggregate.
//    val transformStream = inputStream.streamSimpleEstimate(TIME_COLUMN, TARGET_COLUMN, processedAggregate, isAggregated = true)

    // Write to console.
    val writeStream = transformStream.writeStream
                                     .format("console")
                                     .start()
                                     .awaitTermination(5000)
  }
}

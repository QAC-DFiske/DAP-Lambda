import batch.SimpleEstimationBatch.SimpleEstimationBatch
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FileSystem, FileUtil}
import org.apache.spark.sql.SparkSession

import scala.util.Try

object Runner {

  // Create Spark session.
  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("lambda").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  // Set config constants.
  val TIME_COLUMN = "time"
  val IDENTIFIER_COLUMN = "reporting_unit"
  val TARGET_COLUMN = "turnover"
  val EXTERNAL_PATH = reflect.io.Path("./resources/external")
  val INGESTION_PATH = reflect.io.Path(".resources/ingestion")
  val BATCH_RAW_PATH = reflect.io.Path("./resources/storage/raw")
  val BATCH_PROCESSED_PATH = reflect.io.Path("./resources/storage/processed")

  /**
    * Coalesce all PART files into single file and rename.
    *
    * @param srcPath String - Directory containing PART files.
    * @param dstPath String - Path to save new file (implicitly set name here)
    */
  def merge(srcPath: String, dstPath: String): Unit =  {

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new fs.Path(srcPath), hdfs, new fs.Path(dstPath), false, hadoopConfig, null)
  }

  /**
    * Copy file to new directory.
    *
    * @param srcPath String - Path to file for copying.
    * @param dstPath String - Destination path.
    */
  def copy(srcPath: String, dstPath: String): Unit =  {

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copy(new java.io.File(srcPath), hdfs, new fs.Path(dstPath), false, hadoopConfig)
  }

  def main(args: Array[String]): Unit = {

    // Delete created directory if exists.
    Try(BATCH_PROCESSED_PATH.deleteRecursively)
    Try(INGESTION_PATH.deleteRecursively)

    // Read batch data into DataFrame.
    val batchInput = spark.read.json(BATCH_RAW_PATH + "/batchRaw.json")
    println("Batch input:")
    batchInput.show()

    // Perform SML-like function on DataFrame.
    val batchProcessed = batchInput.simpleEstimate(TIME_COLUMN, TARGET_COLUMN)
    println("Batch processed:")
    batchProcessed.show()

    // Write to temporary directory, coalesce part files into permanent directory, delete temporary directory.
    batchProcessed.write.json(BATCH_PROCESSED_PATH + "/batchProcessedTemp.json")
    merge(BATCH_PROCESSED_PATH + "/batchProcessedTemp.json", BATCH_PROCESSED_PATH.path + "/batchProcessed.json")
    Try(reflect.io.Path(BATCH_PROCESSED_PATH + "/batchProcessedTemp.json").deleteRecursively)

    // Simulate new data arriving into the system.
    copy(EXTERNAL_PATH + "/streamIngest.json", INGESTION_PATH + "/streamIngest.json")
  }
}

import EncryptionUtils.sha256Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object GenerateHashRunner {
    /*
        # Run tests and package into jar
        $ sbt test package

        # Use spark-submit to run your application
        spark-submit \
            --class GenerateHashRunner \
            --master yarn \
            target/scala-2.12/generatehash_2.12-0.1.0-SNAPSHOT.jar
     */
    def main(args: Array[String]) = {
        println("#################################################################################")
        println("\n\n\n")

        val inputFile = "/meesho/distinct-mobile-1-Nov-2023.csv"
        val outputFile = "/meesho/distinct-mobile-1-Nov-2023-hashed.csv"

        val spark = SparkSession
                .builder
                .master("yarn")
                .appName("Generate SHA256 Hash")
                .getOrCreate()

        println("Creating dataframe from distinct-mobile-1-Nov-2023.csv")
        val dataFrame = spark.read.options(
            Map ("header" -> "true",
                "inferSchema" -> "false",
                "mode" -> "failfast")
        ).csv(inputFile)

        println("Total partitions of dataframe: " + dataFrame.rdd.getNumPartitions)
        dataFrame.show(false)

        // Register sha256Hash method as UDF
        val sha256HashUdf = udf(EncryptionUtils.sha256Hash)

        dataFrame.select(
            col("mobile"),
            sha256HashUdf(col("mobile")).as("hash"))
                .show(false)

        dataFrame.repartition(1)
                .write
                .option("header", "true")
                .csv(outputFile)
    }
}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object GenerateHashRunner {
    def main(args: Array[String]) = {
        println("#################################################################################")
        println("\n\n\n")

        val spark = SparkSession
                .builder
                .master("yarn")
                .appName("Generate Mobile Hash")
                .getOrCreate()

        println("Creating dataframe from distinct-mobile-1-Nov-2023.csv")
        val dataFrame = spark.read.options(
            Map ("header" -> "false",
                "inferSchema" -> "true",
                "mode" -> "failfast")
        ).csv("/meesho/distinct-mobile-1-Nov-2023.csv")

        println("Total partitions of dataframe: " + dataFrame.rdd.getNumPartitions)

        dataFrame.show(false)

    }
}

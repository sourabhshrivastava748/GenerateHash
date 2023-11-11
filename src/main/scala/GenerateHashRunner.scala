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

        val unifillMobileNumbersFile = "/meesho/distinct-mobile-1-Nov-2023.csv"
        val meeshoHashCodesFile = "/meesho/meesho_hash_codes.csv"
        val outputFileDirectory = "/meesho/outputdir"
//        val outputFile = "/meesho/hashed-numbers"

        val spark = SparkSession
                .builder
                .master("yarn")
                .appName("Generate SHA256 Hash")
                .getOrCreate()

        println("Creating dataframe from distinct-mobile-1-Nov-2023.csv")
        val unifillDF = spark.read.options(
            Map ("header" -> "true",
                "inferSchema" -> "false",
                "mode" -> "failfast")
        ).csv(unifillMobileNumbersFile)

        // println("Total partitions of dataframe: " + unifillDF.rdd.getNumPartitions)
        // unifillDF.show(false)

        // Register methods as UDF
        val sha256HashUdf = udf(EncryptionUtils.sha256Hash)
        val removePlus91Udf = udf(StringUtils.removePlus91)

//        val unifillHashedDF = unifillDF.select(
//            col("mobile"),
//            sha256HashUdf(col("mobile")).as("hash"))

        val unifillHashedDF = unifillDF.select(
            removePlus91Udf(col("mobile")).as("mobile"),
            sha256HashUdf(removePlus91Udf(col("mobile"))).as("hash")
        )

        unifillHashedDF.show(false)

        val meeshoDF = spark.read.options(
            Map ("header" -> "true",
                "inferSchema" -> "false",
                "mode" -> "failfast")
        ).csv(meeshoHashCodesFile)

        meeshoDF.show(false)

        val dataMatch = unifillHashedDF
                .select(col("mobile") as "unifill_mobile", col("hash") as "unifill_hash")
                .join(meeshoDF, col("unifill_hash") === col("hash"), "inner")
                .select("unifill_mobile","unifill_hash")

        dataMatch.show(200, false)

        val meeshoHashCount = meeshoDF.count()
        val dataMatchCount = dataMatch.count()
        var matchPercentage = (dataMatchCount.toDouble / meeshoHashCount) * 100
        matchPercentage = (matchPercentage * 1000).round / 1000.toDouble

        println("#### meeshoHashCount : " + meeshoHashCount)
        println("#### dataMatchCount : " + dataMatchCount)
        println("#### match percentage : " + matchPercentage)


//        dataMatch.repartition(1)
//                .write
//                .option("header", "true")
//                .csv(outputFileDirectory)
    }
}

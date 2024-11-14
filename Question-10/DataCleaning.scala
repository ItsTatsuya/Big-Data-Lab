import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataCleaning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataCleaning")
      .master("local[*]")
      .getOrCreate()

    try {
      // Read CSV file
      val filePath = "hdfs://localhost:9000/home/hadoop/bigdata/Question-10/titanic.csv"
      val df = spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .csv(filePath)

      println("Original Data:")
      df.show(5, false)

      // Calculate mean age for filling missing values
      val meanAge = df.selectExpr("avg(Age)").first().getDouble(0)

      // Combined cleaning pipeline
      val cleanedDF = df
        // Fill missing values
        .na.fill("Unknown", Array("Cabin"))
        .na.fill(Map(
          "Age" -> meanAge,
          "Embarked" -> "S"
        ))
        // Standardize categorical values
        .withColumn("Sex", lower(col("Sex")))
        .withColumn("Embarked", upper(col("Embarked")))
        // Convert types
        .withColumn("Age", col("Age").cast("double"))
        .withColumn("Fare", col("Fare").cast("double"))
        // Validate critical fields
        .filter(col("Pclass").isin(1, 2, 3))
        .filter(col("Survived").isin(0, 1))

      println("Cleaned Data:")
      cleanedDF.show(5, false)

      // Save cleaned data
      cleanedDF.write
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", "\"")
        .mode("overwrite")
        .csv("hdfs://localhost:9000/home/hadoop/bigdata/Question-10/cleaned_titanic.csv")

      println("Data cleaning completed successfully!")
    } catch {
      case e: Exception => 
        println(s"Error during data cleaning: ${e.getMessage}")
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}
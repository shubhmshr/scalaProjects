package org.diabetes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType




object diabetes extends sessionWrapper {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

        val params = Map(
          "filePath" -> args(0),
          "diabetesFile" -> args(1),
          "admissionSource" -> args(2),
          "admissionType" -> args(3),
          "discharge"->args(4)
        )


        val diabDF: DataFrame = buildDF(params("filePath").toString, params("diabetesFile").toString, diabSchema)

        val admSourceDF: DataFrame = buildDF(params("filePath").toString, params("admissionSource").toString, admissionSourceSchema)

        val admTypeDF: DataFrame = buildDF(params("filePath").toString, params("admissionType").toString, admissionTypeSchema)

        val dischargeDF: DataFrame = buildDF(params("filePath").toString, params("discharge").toString, dischargeSchema)

    diabDF.printSchema()

      // diabDF.write.format("parquet").mode("overwrite").save("/user/shubhmshr4344/test1.parquet")
      // spark-submit --master yarn --class org.diabetes.diabetes scala_spark_2.12-0.1.jar /user/shubhmshr4344/diabetic_data.csv


    }
  }


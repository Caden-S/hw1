import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

val session = SparkSession.builder().getOrCreate()
import session.implicits

object FinalDF {
  def getFB(spark: SparkSession): DataFrame = {
    val data_location = "/datasets/facebook/"
    val df = spark.read.format("csv").option("delimiter", " ").load(data_location)
    print(df.schema)
  }

  def getFBTuples(spark: SparkSession): DataFrame = {
    getFB(sc).map{x => (x(0), x(1))}
  }
} 

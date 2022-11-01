import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object AirportGainLoss {
  def main(args: Array[String]) = {
    val sc = getSC()
    val counts = doGainLoss(sc)
    saveit(counts) 
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("Payment total per country")
    val sc = new SparkContext(conf)
    sc
  }

  def doGainLoss(sc: SparkContext) = {
    val input = sc.textFile("/datasets/flight/")
    val first = input.first()
    val flights = input.filter(row => row != first)
    val result = flights.map(_.split(","))
    val kv = result.flatMap(flight => List((flight(3), -(flight(7).toFloat)), (flight(5), flight(7).toFloat)))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }

  def saveit(counts: org.apache.spark.rdd.RDD[(String, Float)]) = {
    counts.saveAsTextFile("sparklab-q4")
  }
}

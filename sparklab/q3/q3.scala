import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object PaymentTotal {
  def main(args: Array[String]) = {
    val sc = getSC()
    val counts = doPaymentCount(sc)
    saveit(counts) 
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("Payment total per country")
    val sc = new SparkContext(conf)
    sc
  }

  def doPaymentCount(sc: SparkContext) = {
    val input = sc.textFile("/datasets/retailtab/")
    val first = input.first()
    val orders = input.filter(row => row != first)
    val orders2 = orders.map(_.split("\t"))
    val result = orders2.filter(_(3).toFloat > 0)
    val kv = result.map(order => (order(7), order(3).toFloat * order(5).toFloat))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }

  def saveit(counts: org.apache.spark.rdd.RDD[(String, Float)]) = {
    counts.saveAsTextFile("sparklab-q3")
  }
}

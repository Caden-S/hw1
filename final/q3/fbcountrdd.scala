import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object facebookReducer {
  def main(args: Array[String]) = {
    val sc = getSC()
    val counts = facebookReduce(sc)
    saveit(counts) 
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("Facebook node reduction")
    val sc = new SparkContext(conf)
    sc
  }

  def facebookReduce(sc: SparkContext) = {
    val input = sc.textFile("/datasets/facebook/")
    val result = input.map(_.split(" "))
    val result2 = result.filter(_(1).toInt > 500)
    val kv = result2.map(node => (node(0), 1))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }

  def saveit(counts: org.apache.spark.rdd.RDD[(String, Int)]) = {
    counts.saveAsTextFile("fbcount_rdd")
  }
}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

val session = SparkSession.builder().getOrCreate()
import session.implicits

object DFlab {
    def getTestDataFrame(session: SparkSession): DataFrame = {
      List((1,2,3), (4,5,6)).toDF("a", "b", "c")
    }

    def getFB(spark: SparkSession): DataFrame = {
      val data_location = "/datasets/facebook" 
      val df = spark.read.format("csv").option("delimiter", " ").load(data_location)
      print(df.schema)
    }

    def getFBTuples(spark: SparkSession): DataFrame = {
        getFB(sc).map{x => (x(0), x(1))}
    }

    def examine_rdd[T](mydata: DataFrame, numlines: Int = 5): Unit = {
        val small_local_array = mydata.take(numlines)
        small_local_array.foreach{x => println(x)}
    }


    def adj_or_edgelist(fb: DataFrame): Boolean = {
        val length_data  = fb.map{x => x.length}  // convert an array to its length
        val filtered_data = length_data.filter{x => x != 2}  // rows that do not look like edge lists
        val num = filtered_data.count() // how many rows do not look like edge lists
        num != 0       
    }

    def is_redundant(fb: DataFrame): Boolean = {
       // step 0: quality control, get rid of duplicate rows
       val clean_fb = fb.distinct()       
       // step 1: flip the order of each tuple
       // val flipped = fb.map{x => (x._2, x._1)} //yuckier version of next line
       val flipped = clean_fb.map{case (a, b) => (b, a)} 
       val common = flipped.intersection(clean_fb) //finds rows that are in fb and in flipped
                                             // but also removes duplicates in the answer
       val count_original = clean_fb.count()
       val count_common = common.count()
       count_original == count_common      
    }


    def to_non_redundant(fb: DataFrame): DataFrame = {
       val result = fb.filter{case (a,b) => a < b}.distinct()
       result
    }

    def toyGraph(spark: SparkSession): DataFrame = {
       val mylist = List(
                   ("A", "B"),
                   ("B", "A"),
                   ("A", "C"),
                   ("B", "C"),
                   ("A", "D"),
                   ("C", "E"),
                   ("D", "C"),
                   ("C", "A"),
                   ("C", "B"),
                   ("D", "A"),
                   ("C", "D"),
                   ("E", "C")
        )

        val mydata = sc.parallelize(mylist, 2) // take the input list, turn it into an RDD, using
                                              // 2 workers

        mydata
    }
  
    def numTriangles(graph: DataFrame) = {
        // whatever the input graph is, we want to make sure we are
        // working with a redundant version, with duplicates removed
        val flipped = graph.map{case (a, b) => (b, a)}
        val combined = graph.union(flipped).distinct()
      
        val selfjoin = combined.join(combined)       
        
        val cleaned = selfjoin.filter{case (mid, (start, end)) => start != end}
        
        val flipped_clean = cleaned.map{case (a, b) => (b,a)}
        
        val hacked_combined = combined.map{x => (x, 1)}
        
        val all_joined = flipped_clean.join(hacked_combined)
        
        all_joined.count() / 6
    }  
}

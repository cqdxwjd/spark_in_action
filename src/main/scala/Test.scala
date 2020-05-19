import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    val list = List.fill(500)(scala.util.Random.nextInt(100))
    val rdd = sc.parallelize(list, 30).glom()
  }
}

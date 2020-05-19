package chapter03

import org.apache.spark.sql.SparkSession

import scala.io.Source.fromFile

object GitHubDay {
  def main(args: Array[String]): Unit = {
    // local
    //    val spark = SparkSession.builder().appName("GitHub push counter").master("local[*]").getOrCreate()
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val ghLog = spark.read.json(args(0))
    val pushes = ghLog.filter("type='PushEvent'")

    val grouped = pushes.groupBy("actor.login").count()
    grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
      )
    val bcEmployees = sc.broadcast(employees)
    import spark.implicits._
    val isEmp = {
      bcEmployees.value.contains(_)
    }
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.write.format(args(3)).save(args(2))
    //    filtered.show()
    //    pushes.printSchema()
    //    println("all events: " + ghLog.count())
    //    println("only pushes: " + pushes.count())
    //    pushes.show(5)
  }
}

package chapter03

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GitHub push counter").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val ghLog = spark.read.json("C:/Users/Administrator/Desktop/2015-03-01-0.json")
    val pushes = ghLog.filter("type='PushEvent'")

    val grouped = pushes.groupBy("actor.login").count()
    grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    val employees = Set() ++ (
      for {
        line <- fromFile("C:/Users/Administrator/Desktop/ghEmployees.txt").getLines
      } yield line.trim
      )
    val bcEmployees = sc.broadcast(employees)
    import spark.implicits._
    val isEmp = {
      bcEmployees.value.contains(_)
    }
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()
    //    pushes.printSchema()
    //    println("all events: " + ghLog.count())
    //    println("only pushes: " + pushes.count())
    //    pushes.show(5)
  }
}

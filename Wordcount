package com.iitjobs.sparkapps
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object wordcount {
   def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
      val sc = new SparkContext(conf)

      
val word = Console.readLine
println("You entered " + word)
val input = sc.textFile("input.txt")
val splitedLines = input.flatMap(line => line.split(" "))
                    .filter(x => x.equals(word))

System.out.println(splitedLines.count())
}
}

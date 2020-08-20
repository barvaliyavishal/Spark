package test
import org.apache.spark.SparkContext
import org.apache.log4j._

object WordCount  extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","test")
  val book = sc.textFile("src/data/book.txt")
  val words = book.flatMap(x => x.split("\\W+"))
  val Lowercase = words.map(x => x.toLowerCase())

  val mappedWords = Lowercase.map(x => (x,1)).reduceByKey((x,y) => x+y)
  val sortedWords = mappedWords.map(x => (x._2,x._1)).sortByKey()

  val finalWords = sortedWords.filter(x => x._2!="the" && x._2!="a" && x._2!="an" && x._2!="you" && x._2!="that" && x._2!="then" && x._2!="does" && x._2!="do" && x._2!="did" && x._2!="done" && x._2!="or" && x._2!="and" )
  finalWords.foreach(println)
  println( "before filter " + sortedWords.count())
  println("after filter " + finalWords.count())
}

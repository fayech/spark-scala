/**
  * Created by student on 12/22/20.
  */
import org.apache.spark._
import scala.io.Source;
object Main {
  def main(args: Array[String]): Unit = {
    /*
    val fileRead= Source.fromFile("/home/student/data/twinkle/sample.txt");
    fileRead.foreach(print);
    */
    val conf = new SparkConf()

    conf.setAppName("Datasets Test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    println(sc)
    val url="/home/student/data/twinkle/sample.txt"

    val file = sc.textFile(url)

    println(file.getClass)
    file.foreach(println)
     val dataflat= file.flatMap(line => line.split(" "))
       dataflat.foreach(println)
  val words=  dataflat.map(word=>(word,1))
    words.foreach(println)
    val freq=words.reduceByKey(_+_)
    freq.foreach(println)
    val rddreduce= file.reduce((a,b)=>a+'\t'+b)
 rddreduce.foreach(println)
  }
}


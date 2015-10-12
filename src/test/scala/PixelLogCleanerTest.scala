/**
 * Created by root on 10/9/15.
 */
package org.merkle.mde
import org.apache.spark.{SparkContext,SparkConf}
import org.scalatest._
import java.io._
import scala.io.Source
import java.util.Calendar

class PixelProcessorTest extends FlatSpec with Matchers {
  val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.driver.allowMultipleContexts", "false")
  val sc = new SparkContext(conf)
  val in = java.io.File.createTempFile(System.getProperty("user.dir") + "/input", "txt")
  val out = System.getProperty("user.dir") + "/pixel_out"
  val validString = "1\t2\t3\t4\t5\t6\t20150925184029.185\t8\t9\t10\t11\t12\t13\t14\t15"
  val invalidString = "1\t2\t3\t4\t5\t6\t20150garblegarble29.185\t8\t9\t10\t11\t12\t13\t14\t15"
  val validOut = "1\t2\t3\t4\t5\t6\t2015-09-25 18:40:29\t8\t9\t10\t11\t12\t13\t14\t15"
  val invalidOut = "1\t2\t3\t4\t5\t6\t1900-01-01 00:00:00\t8\t9\t10\t11\t12\t13\t14\t15"
  "PixelLineCleaner" should "return a string with a properly formatted date" in {
    val inputLine = validString
    val outputLine = PixelLineCleaner.processLine(inputLine)
    outputLine should equal (validOut)
  }
  "PixelLineCleanerInvalid" should "return a string with a default date when only date format is incorrect" in {
    val inputLine = invalidString
    val outputLine = PixelLineCleaner.processLine(inputLine)
    outputLine should equal (invalidOut)
  }
  "PixelFileCleaner" should "result in a file containing a properly formatted date" in {
    val bw = new BufferedWriter(new FileWriter(in))
    bw.write(validString)
    bw.close()
    val outString =  out + "/" + Calendar.getInstance().getTime().toString()
    val lc = new PixelLogCleaner(sc,in.getAbsolutePath(),outString)
    lc.CleanJob()
    var l = Array[String]()
    for (line <- Source.fromFile(outString + "/part-00000").getLines){
      l = line.split("\t")
    }
    l(6) should equal ("2015-09-25 18:40:29")
  }
}
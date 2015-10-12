/**
 * Created by Dan Mahaney on 10/9/15.  Implements the logic of the spark job
 */
package org.merkle.mde
import java.text._
import org.apache.spark.SparkContext

object PixelLineCleaner{  //extract into object, so that the class is not serialized on map
val formatOld = new SimpleDateFormat("yyyyMMddHHmmss.SSS")
  val formatNew = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val errorResult = "1900-01-01 00:00:00"
  def processLine(in:String):String = {
    val inArray = in.split("\t")
    try {
      inArray(6) = formatNew.format(formatOld.parse(inArray(6))) //transform date to hive friendly format
    }
    catch{
      case e:ParseException => inArray(6) = errorResult
    }
    inArray.mkString("\t")//Return date string
  }
}
class PixelLogCleaner(sparkContext: SparkContext, input:String,output:String)
{
  def CleanJob() = {
    val logOld = sparkContext.textFile(input)
    logOld.map(line => PixelLineCleaner.processLine(line)).saveAsTextFile(output)
  }
}
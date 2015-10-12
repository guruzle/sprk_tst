/**
 * Created by Daniel Mahaney on 10/9/15.
 * Base Object for executing spark program
 */
package org.merkle.mde

import org.apache.spark.{SparkConf, SparkContext}
object PixelLogProcessorApp {
  def main(args:Array[String]){
    //create mutable configuration based variables
    var input = ""
    var output = ""
    var location = "local"
    //create spark objects
    val conf = new SparkConf().setAppName("Pixel Log Processor")
    val sc = new SparkContext(conf)
    //process command line arguments
    val utility = new PixelLogProcessorUtilities()
    utility.mapArguments(args).foreach(arg => arg._1 match {
      case "--key" => sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", arg._2)
      case "--secret_key" => sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", arg._2)
      case "--input" => input = arg._2
      case "--output" => output = arg._2
      case "--location" => location = arg._2
      case _ =>
    })
    if (input == "" || output == ""){
      println("Must declare --input and --output")
    }
    else{
      if (location == "aws"){ //deploying to AWS, and will use prog. defined date structure
        //input = input + utility.buildDatedDirectory() //commented out for demo
        output = output + utility.buildDatedDirectory()
      }
      val plc = new PixelLogCleaner(sc,input,output)
      plc.CleanJob()
    }
  }
}
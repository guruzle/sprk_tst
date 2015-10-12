/**
 * Created by Dan Mahaney on 10/9/15.
 * Utilities for handling arguments, building directories, unrelated to actual Spark tasks
 */
package org.merkle.mde

import java.text.SimpleDateFormat
import java.util.Calendar

class PixelLogProcessorUtilities {
  def buildDatedDirectory(): String = {
    //builds a directory structure for in and out locales, based on current day
    val sdf = new SimpleDateFormat("dd-MM-yyyy")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1) //looking for yesterday's data
    val dt = sdf.format(cal.getTime).split("-")
    val year = dt(2)
    val month = dt(1)
    val day = dt(0)
    s"/year=$year/month=$month/day=$day/"
  }
  def mapArguments(arguments:Array[String]):scala.collection.mutable.Map[String,String] = {
    var argumentMap=  scala.collection.mutable.Map[String,String]()
    arguments.sliding(2)foreach(a => argumentMap += (a(0) -> a(1)))
    argumentMap
  }
}
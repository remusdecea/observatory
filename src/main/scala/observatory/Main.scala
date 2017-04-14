package observatory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object Main extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Observatory")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  Extraction.locateTemperatures(1975, "stations.csv", null)
}

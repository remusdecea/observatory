package observatory

import scala.io.Source
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

/**
  * 1st milestone: data extraction
  */
object Extraction {

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    def getLinesFromFile(filename: String) = {
      //Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)).getLines()
       Main.sc.textFile(filename)
    }

    def fahrenheitToCelsius(farenheit: Double) = {
      ((BigDecimal(farenheit) - 32) * 5 / 9).toDouble
    }
    // if will use spark, partition by Station case class

    val stationLines = getLinesFromFile(stationsFile)
    val stations = stationLines
      .map(_.split(",", -1))
      .filter(values => values.length == 4 && !values(2).isEmpty && !values(3).isEmpty)
      .map(values => (Station(values(0), values(1)), Location(values(2).toDouble, values(3).toDouble)))

    val temperaturesLines = getLinesFromFile(temperaturesFile)
    val temperatures = temperaturesLines
        .map(_.split(",", -1)).filter(values => values.length == 5) //TODO: maybe remove this
        .map(values => (
            Station(values(0), values(1)),
            (LocalDate.of(year, values(2).toInt, values(3).toInt),
              fahrenheitToCelsius(values(4).toDouble)))
        )

    val res = stations.join(temperatures).map{
      case (station, (location, (date, temperature))) => (date, location, temperature)
    }

    res.collect().toSeq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    val recordsRdd: RDD[(LocalDate, Location, Double)] = Main.sc.parallelize(records.toList)
    val map = recordsRdd.map(rec => (rec._2, (rec._3, 1)))
    val key: RDD[(Location, (Double, Int))] = map.reduceByKey{case (x, y) => (x._1 + y._1, x._2 + y._2)}
    val values: RDD[(Location, Double)] = key.mapValues{case (sum, count) => sum/count}
    val res = values.collect().toList
    res
  }

}

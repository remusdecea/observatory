package observatory

import scala.io.Source
import java.time.LocalDate

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
      Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)).getLines()
    }

    // if will use spark, partition by Station case class

    val stationLines = getLinesFromFile(stationsFile)
    val stations = stationLines
      .map(_.split(",", -1))
      .filter(values => values.length == 4 && !values(2).isEmpty && !values(3).isEmpty)
      .map(values => (Station(values(0), values(1)), Location(values(2).toDouble, values(3).toDouble)))
      .toMap

    val temperaturesLines = getLinesFromFile(temperaturesFile)
    val temperatures = temperaturesLines
        .map(_.split(",", -1)).filter(values => values.length == 5) //TODO: maybe remove this
        .map(values => (
            Station(values(0), values(1)),
            (LocalDate.of(year, values(2).toInt, values(3).toInt), values(4).toDouble))
        )
        .toMap

    //if will move to spark, here would be a join between the two
    val merged: Map[Station, Seq[(Station, Product with Serializable)]] = (stations.toSeq ++ temperatures.toSeq).groupBy(_._1)
    val cleaned = merged.mapValues(_.map(_._2).toList)

    ???
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    ???
  }

}

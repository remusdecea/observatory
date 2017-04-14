package observatory

import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.log4j.{Level, Logger}

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {


  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  test("locate temperatures should list all records in Celsius with date and location"){
    val stationsFileName = getClass.getClassLoader.getResource("stations.csv").getPath
    val testsFileName = getClass.getClassLoader.getResource("test.csv").getPath

    val temperatures = Extraction.locateTemperatures(2015, stationsFileName, testsFileName)

    val expectedTemperatutes = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    )

    assert(temperatures == expectedTemperatutes)
  }
}
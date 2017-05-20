package observatory

import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import Extraction._
import org.apache.spark.sql.Row

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {
  val year = LocalDate.of(2020, 1, 1).getYear
  val stationsFileName = "/stations_test1.csv"
  val temperaturesFileName = "/" + Integer.toString(year) + ".csv"

  test("Load and transform temperatures ") {
    val actual = temperatureData(year, temperaturesFileName).take(1)
    actual.foreach(println(_))
    assert(actual.length == 1 && actual(0).equals(Row("010013", "", year, 11, 25, 4.000000000000002)))
  }

  test("Exctract location temperatures from year and files") {

    val result = locateTemperatures(year, stationsFileName, temperaturesFileName)

    val expected =
      Seq(
        (LocalDate.of(year, 8, 11), Location(37.35, -78.433), 27.3),
        (LocalDate.of(year, 12, 6), Location(37.358, -78.438), 0.0),
        (LocalDate.of(year, 1, 29), Location(37.358, -78.438), 2.000000000000001)
      )

    val matched = result.flatMap(oneResult => expected.filter(oneExpected =>
      (oneExpected._1.equals(oneResult._1)) && (oneExpected._2.equals(oneResult._2)))
      .map(oneFiltered => (oneFiltered._3, oneResult._3)))
    assert(result.size == expected.size && result.size == matched.size)

    matched.foreach(entry => assert(entry._1 == entry._2))

  }

  test("Average year temperatures") {
    val records: Iterable[(LocalDate, Location, Double)] = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 2, 11), Location(37.35, -78.433), 11.1),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    )

    val expected: Iterable[(Location, Double)] = Seq(
      (Location(37.35, -78.433), 19.2),
      (Location(37.358, -78.438), 1.0)
    )
    val actual: List[(Location, Double)] = locationYearlyAverageRecords(records).toList

    expected.map(one => assert(actual.contains(one)))

  }

  test("Messed up stations IDs") {
    val result = locateTemperatures(2022, "/stations_test2.csv", "/2022.csv")
    result.foreach(println(_))
    val expected = Set(LocalDate.of(2022,1,1),Location(1.0,-1.0),10.0)
    (LocalDate.of(2022,1,2),Location(2.0,-2.0),10.0)
    (LocalDate.of(2022,1,3),Location(3.0,-3.0),10.0)
    (LocalDate.of(2022,1,5),Location(5.0,-5.0),10.0)
    (LocalDate.of(2022,1,4),Location(4.0,-4.0),10.0)

    assert(result.size === 5)
  }

}
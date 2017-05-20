package observatory

import java.time.LocalDate


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.reflect.ClassTag

/**
  * 1st milestone: data extraction
  */
object Extraction {
  type STNColumn = String
  type WBANColumn = String
  val stationColumns = ("STN identifier", "WBAN identifier", "Latitude", "Longitude")
  val temperaturesColumns = ("STN identifier", "WBAN identifier", "Month", "Day", "Temperature")
  private val structFieldSTN = StructField("STN", StringType)
  private val structFieldWBAN = StructField("WBAN", StringType)
  private val structFieldYear = StructField("Year", IntegerType)
  private val structFieldMonth = StructField("Month", IntegerType)
  private val structFieldDay = StructField("Day", IntegerType)
  private val structFieldTemperature = StructField("Temperature", DoubleType)
  val emptyStringColumn = lit("")
  val temperaturesSchema: StructType =
    StructType(List(structFieldSTN, structFieldWBAN,
      structFieldYear, structFieldMonth, structFieldDay,
      structFieldTemperature))
  /*
    |-- STN: string (nullable = true)
    |-- WBAN: string (nullable = true)
    |-- Year: integer (nullable = true)
    |-- Month: integer (nullable = true)
    |-- Day: integer (nullable = true)
    |-- Temperature: double (nullable = true)
  */
  private val structFieldLatitude = StructField("Latitude", DoubleType)
  private val structFieldLongitude = StructField("Longitude", DoubleType)
  val stationsSchema = StructType(List(structFieldSTN, structFieldWBAN, structFieldLatitude, structFieldLongitude))

  val columnSTN = 0
  val columnWBAN = 1
  val columnLatitude = 2
  val columnLongitude = 3
  val columnMonth = 2
  val columnDay = 3
  val columnTemperature = 4
  val missingTemperatureValue: Double = 9999.9
  val stationColumnsNumber = 4
  val temperatureColumnsNumber = 5
  val fileNameResourcePrefix = "/"
  val fileNameExtension = ".csv"
  val temperaturesViewName = "temperatures"

  val sqlColumnSTN = column(structFieldSTN.name)
  val sqlColumnWBAN = column(structFieldWBAN.name)
  val sqlColumnYear = column(structFieldYear.name)
  val sqlColumnMonth = column(structFieldMonth.name)
  val sqlColumnDay = column(structFieldDay.name)
  val sqlColumnTemperature = column(structFieldTemperature.name)
  val sqlColumnLatitude = column(structFieldLatitude.name)
  val sqlColumnLongitude = column(structFieldLongitude.name)


  val splitChar = ","

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("Temperature Data").setMaster("local[*]")
  val sparkSession = SparkSession.builder().config(conf).getOrCreate
  val sc = sparkSession.sparkContext

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    Encoders.kryo[A](ct)


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    println(s"Temperatures file name $temperaturesFile")

    val stations = stationsData(stationsFile)
    stations.show(10)
    val temperatures = temperatureData(year, temperaturesFile)

    temperatures.show(10)

    val joinedByStationIDs = temperatures.join(stations,
      ((stations(structFieldSTN.name) =!= emptyStringColumn && stations(structFieldWBAN.name) =!= emptyStringColumn) &&
        stations(structFieldSTN.name) === temperatures(structFieldSTN.name) && stations(structFieldWBAN.name) === temperatures(structFieldWBAN.name))
        ||
        ((stations(structFieldSTN.name) =!= emptyStringColumn &&
          temperatures(structFieldSTN.name) =!= emptyStringColumn &&
          stations(structFieldWBAN.name) === emptyStringColumn &&
          temperatures(structFieldWBAN.name) === emptyStringColumn) &&
          stations(structFieldSTN.name) === temperatures(structFieldSTN.name))
        ||
        ((stations(structFieldSTN.name) === emptyStringColumn &&
          temperatures(structFieldSTN.name) === emptyStringColumn &&
          stations(structFieldWBAN.name) =!= emptyStringColumn &&
          temperatures(structFieldWBAN.name) =!= emptyStringColumn) &&
          stations(structFieldWBAN.name) === temperatures(structFieldWBAN.name))
    )
    joinedByStationIDs.show(10)

    val distinctValues = joinedByStationIDs.select(sqlColumnYear, sqlColumnMonth, sqlColumnDay, sqlColumnTemperature, sqlColumnLatitude, sqlColumnLongitude)
//      .distinct

    val aggregated = distinctValues
      .groupBy(sqlColumnYear, sqlColumnMonth, sqlColumnDay, sqlColumnLatitude, sqlColumnLongitude)
      .agg(avg(sqlColumnTemperature).as(structFieldTemperature.name))

    val result = aggregated
      .map(row => (LocalDate.of(row.getInt(0), row.getInt(1), row.getInt(2)), Location(row.getDouble(3), row.getDouble(4)), row.getDouble(5)))

    result.collect.toIterable
  }

  def temperatureData(year: Int, temperaturesFile: String) = {

    val temperaturesRDD = sc.textFile(getURI(temperaturesFile).getPath)
      .map(line => splitCsvLine(line))
      .filter(validTemperature(_))
      .map(convertTemperature(_, year))

    val temperaturesDF = sparkSession.createDataFrame(temperaturesRDD, temperaturesSchema)
    println("Number of raw temperature records " + temperaturesDF.collect().length)
    temperaturesDF
  }

  def stationsData(stationsFile: String) = {
    println(s"Stations file name $stationsFile")
    val stations = sc.textFile(getURI(stationsFile).getPath)
      .map(line => splitCsvLine(line))
      .filter(validStations(_))
      .map(convertStation(_))

    sparkSession.createDataFrame(stations, stationsSchema)
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    val mapped = records.groupBy(_._2)
    mapped.map {
      case (location, values) => (location, values.foldLeft(0.0)((acc, value) => value._3 + acc) / values.size)
    }
  }

  def splitCsvLine(line: String) = {
    line.split(splitChar, -1)
  }

  private def getURI(fileName: String) = {
    val uri = getClass.getResource(fileName).toURI
    uri
  }

  private def fahrenheitToCelsius(f: Double): Double =
    (f - 32.0) * (5.0 / 9.0)

  private def validStations(station: Array[String]) = station.size == stationColumnsNumber && !(station(columnLatitude).isEmpty || station(columnLongitude).isEmpty)

  def convertStation(array: Array[String]) = {
    Row(array(columnSTN), array(columnWBAN), array(columnLatitude).toDouble, array(columnLongitude).toDouble)
  }

  private def convertTemperature(array: Array[String], year: Int) = {
    Row(array(columnSTN), array(columnWBAN), year, array(columnMonth).toInt, array(columnDay).toInt, fahrenheitToCelsius(array(columnTemperature).toDouble))
  }

  private def validTemperature(temperatureLine: Array[String]): Boolean = {
    temperatureLine.size == temperatureColumnsNumber && !temperatureLine(columnTemperature).isEmpty &&
      temperatureLine(columnTemperature).toDouble != missingTemperatureValue
  }
}

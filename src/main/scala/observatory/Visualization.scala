package observatory

import com.sksamuel.scrimage.{Image, Pixel, RGBColor}
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}

import scala.util.{Failure, Success}
import akka.{NotUsed}
import akka.actor.ActorSystem
import scala.concurrent._
import scala.concurrent.duration._
import Extraction.sc

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  implicit val system = ActorSystem("observatory")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher


  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    val source: Source[(Location, Double), NotUsed] = Source(temperatures.toList)
    val mapping: Flow[(Location, Double), (Double, Double), NotUsed] = Flow[(Location, Double)].map {
      case (oneLocation, temperature) => {
        (distanceInKm(oneLocation, location), temperature)
      }
    }

    val reduceSink: Sink[(Double, Double), Future[(Double, Double)]] = Sink.fold[(Double, Double), (Double, Double)]((0.0, 0.000000000001)) {
      case ((accTemperature, accWeight), (distance, temperature)) => {
        val weight = weighing(distance)
        (accTemperature + temperature * weight, accWeight + weight)
      }
    }

    val counterGraph: RunnableGraph[Future[(Double, Double)]] =
      source
        .via(mapping)
        .toMat(reduceSink)(Keep.right)

    /*
        val example1 = source.via(mapping).runWith(Sink.head)
        val example2 = source.via(mapping).runWith(reduceSink)
    */

    val future: Future[(Double, Double)] = counterGraph.run()

    val promise = Await.ready(future, Duration.Inf)
    val promiseTry = promise.value.get
    promiseTry match {
      case Success(_) =>
        system.terminate()
      case Failure(exception) => {
        system.terminate()
        throw exception
      }
    }

    val (temperatureSum, weightsSum) = promiseTry.get
    temperatureSum / weightsSum
  }

  def weighing(distance: Double) = if (distance <= 1.0) 1.0 else 1.0 / Math.pow(distance, p)

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {
    val exact = points.filter(point => point._1 == value)
    if (exact.size > 0) (exact head)._2
    else {
      val minPointValue = points.minBy(_._1)
      val maxPointValue = points.maxBy(_._1)
      if (value <= minPointValue._1) minPointValue._2
      else if (value >= maxPointValue._1) maxPointValue._2
      else {
        val (closest1, closest2) = points.foldLeft((Double.MaxValue, (0.0, new Color(0, 0, 0))), (Double.MaxValue, (0.0, new Color(0, 0, 0))))((acc, entry) => {
          val newDiff = Math.abs(value - entry._1)
          val closerThenFirst = newDiff < acc._1._1
          val betweenFirstAndSecond = newDiff >= acc._1._1 && newDiff < acc._2._1
          if (betweenFirstAndSecond) (acc._1, (newDiff, entry))
          else if (closerThenFirst) ((newDiff, entry), acc._1)
          else acc
        })
        colorFromTwoPoints(closest1._2, closest2._2, value)
      }
    }
  }

  def distanceDoubles(double1: Double, double2: Double) = Math.abs(double1 - double2)

  def colorFromTwoPoints(point1: (Double, Color), point2: (Double, Color), newTemperature: Double): Color = {
    val xDiff1 = point2._1 - newTemperature
    val xDiff2 = newTemperature - point1._1
    val xDiff3 = point2._1 - point1._1
    val newR = forY(xDiff1, xDiff2, xDiff3, point1._2.red, point2._2.red)
    val newG = forY(xDiff1, xDiff2, xDiff3, point1._2.green, point2._2.green)
    val newB = forY(xDiff1, xDiff2, xDiff3, point1._2.blue, point2._2.blue)
    new Color(newR, newG, newB)
  }

  private def forY(xDif1: Double, xDif2: Double, xDif3: Double, y0: Int, y1: Int): Int = {
    val value = ((y0 * xDif1 + y1 * xDif2) / xDif3)
    Math.round(value).toInt
  }

  implicit val locationOrdering = new Ordering[Location] {
    override def compare(x: Location, y: Location): Int = {
      val compLat = x.lat.compare(y.lat)
      if (compLat != 0) compLat else x.lon.compare(y.lon)
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    println(s"visualize colors length ${colors.size} temperatures length ${temperatures.size}")
    val allLocations: Iterable[Location] = for {x <- topLeft.lat.toInt to bottomRight.lat.toInt
                                                y <- topLeft.lon.toInt to bottomRight.lon.toInt
    } yield new Location(x.toDouble, y.toDouble)

    val allocationsRDD = sc.parallelize(allLocations.toSeq)

    val allTemperatures = allocationsRDD map (location => (location, predictTemperature(temperatures, location)))
    val allColors = allTemperatures map { case (location, temperature) =>
      val color = interpolateColor(colors, temperature)
      (location, color)
    }
    val sorted = allColors.sortBy(entry => entry._1).map(entry => entry._2).collect

    Image(360, 180, sorted.map(color => new RGBColor(color.red, color.green, color.green).toPixel))
  }

  private def degreesToRadians(degrees: Double) = {
    degrees * Math.PI / 180.0
  }

  def distanceInKm(location1: Location, location2: Location): Double = {
    val dLat = degreesToRadians(location2.lat - location1.lat)
    val dLon = degreesToRadians(location2.lon - location1.lon)

    val lat1Rad = degreesToRadians(location1.lat)
    val lat2Rad = degreesToRadians(location2.lat)

    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1Rad) * Math.cos(lat2Rad)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    earthRadiusKM * c
  }

  private val p = 4
  private val earthRadiusKM = 6371.00
  private val topLeft = Location(90, -180)
  private val bottomRight = Location(-90, 180)
}


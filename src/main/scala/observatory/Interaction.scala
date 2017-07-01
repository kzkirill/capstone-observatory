package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math._
import Visualization._
import observatory.TilesGenerator.{LatLonPoint, Tile}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  /**
    * @param zoom Zoom level
    * @param x    X coordinate
    * @param y    Y coordinate
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(zoom: Int, x: Int, y: Int): Location = {
    println(s"tileLocation zoom: $zoom, x: $x, y: $y")
    val n = pow(2, zoom)
    val lonDeg = x / n * 360 - 180
    val latDeg = atan(sin(Pi - y / n * 2 * Pi)) * 180 / Pi
    new Location(if (latDeg == 0.0) 85.0511 else latDeg, lonDeg)
  }

  val defaultTileHeight = 256

  val defaultTileWidth = 256

  val defaultAlpha: Int = 127

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param zoom         Zoom level
    * @param x            X coordinate
    * @param y            Y coordinate
    * @return A 256×256 image showing the contents of the tile defined by `x`, `y` and `zooms`
    */
  def tile(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)], zoom: Int, x: Int, y: Int): Image = {
    val tileLoc = tileLocation(x, y, zoom)
    val twoToPowerOfZoom = math.pow(2, zoom)
    val latitudeSpacing = 180d / (twoToPowerOfZoom * defaultTileHeight)
    val longitudeSpacing = 360d / (twoToPowerOfZoom * defaultTileWidth)
    val tileCorner = LatLonPoint(tileLoc.lat, tileLoc.lon, zoom.toShort).toTile

    val coords = for {
      yCoord <- 0 until defaultTileHeight
      xCoord <- 0 until defaultTileWidth
      currentY = tileCorner.y - yCoord * latitudeSpacing
      currentX = tileCorner.x + xCoord * longitudeSpacing
      gpsPoint = Tile(currentX.toInt, currentY.toInt, zoom.toShort).toLatLon
    } yield (xCoord, yCoord, Location(gpsPoint.lat, gpsPoint.lon))

    val pixels = coords.toParArray.map { case (xCoord, yCoord, location) =>
      val predTemp = predictTemperature(temperatures, location)
      val predColor = interpolateColor(colors, predTemp)
      Pixel(predColor.red, predColor.green, predColor.blue, defaultAlpha)
    }

    Image(defaultTileWidth, defaultTileHeight, pixels.toArray)
  }

  private def locationInTile(tileL: Location, location: Location, n: Double) = {
    val width = 360 / n
    val height = 180 / n
    tileL.lat >= location.lat && tileL.lon >= location.lon && (tileL.lat + height) <= location.lat && (tileL.lon + width) <= location.lon
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
                           yearlyData: Iterable[(Int, Data)],
                           generateImage: (Int, Int, Int, Int, Data) => Unit
                         ): Unit = {
    ???
  }

}

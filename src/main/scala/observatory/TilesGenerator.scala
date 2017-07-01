package observatory

import scala.math._

/**
  * Created by Kirill on 6/24/2017.
  */
object TilesGenerator {

  case class Tile(x: Int, y: Int, z: Short) {
    def toLatLon = new LatLonPoint(
      toDegrees(atan(sinh(Pi * (1.0 - 2.0 * y.toDouble / (1 << z))))),
      x.toDouble / (1 << z) * 360.0 - 180.0,
      z)

    def toURI = new java.net.URI("http://tile.openstreetmap.org/" + z + "/" + x + "/" + y + ".png")
  }

  case class LatLonPoint(lat: Double, lon: Double, z: Short) {
    def toTile = new Tile(
      ((lon + 180.0) / 360.0 * (1 << z)).toInt,
      ((1 - log(tan(toRadians(lat)) + 1 / cos(toRadians(lat))) / Pi) / 2.0 * (1 << z)).toInt,
      z)
  }

}

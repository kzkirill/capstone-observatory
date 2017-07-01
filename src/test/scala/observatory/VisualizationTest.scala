package observatory


import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers
import observatory.Visualization._
import observatory.Extraction._
import observatory.Extraction.sc
import org.apache.spark.rdd.RDD

@RunWith(classOf[JUnitRunner])
class VisualizationTest extends FunSuite with Checkers {
  val telaviv = Location(32.109333, 34.855499)
  val lessTnehkm = Location(32.109333, 34.855490)
  val batyam = Location(32.017136, 34.745441)
  val holon = Location(32.0166666, 34.7666636)
  val haifa = Location(32.794044, 34.989571)

  /*
    test("fold") {
      def reduceyWeight (rdd: RDD[(Double,Double)])= {
        val transformedRDD = rdd.map {
          case (temp, distance) => {
            val weight = weighing(distance)
            (temp * weight, weight)
          }
        }
        val reduced = transformedRDD.reduce {
          case ((accTemp, accWeight), (temp, weight)) => (accTemp + temp, accWeight + weight)
        }
        println(reduced._1 / reduced._2)
      }

      val rdd1 = sc.parallelize(List((50.0, 122.0), (60.0, 2.0)))
      reduceyWeight(rdd1)
      val rdd2 = sc.parallelize(List((50.0, 12.0), (60.0, 132.0)))
      reduceyWeight(rdd2)
      val rdd3 = sc.parallelize(List((50.0, 12.0), (60.0, 12.0)))
      reduceyWeight(rdd3)
      val rdd4 = sc.parallelize(List((50.0, 2.0), (60.0, 4.0)))
      reduceyWeight(rdd4)
      val rdd5 = sc.parallelize(List((50.0, 300.0), (60.0, 200.0)))
      reduceyWeight(rdd5)
      val rdd6 = sc.parallelize(List((50.0, 100.0), (60.0, 200.0)))
      reduceyWeight(rdd6)
    }

    test("Distance in KM test") {
      val london = Location(51.5, 0)
      val arlington = Location(38.8, -77.1)

      assert(Math.abs(0.0 - distanceInKm(Location(1, 1), Location(1, 1))) <= 0.0)
      val value1 = distanceInKm(london, arlington)
      assert(5918 - value1.toInt === 0)

      val distance = 14
      assert(distance === distanceInKm(telaviv, batyam).toInt)
      val lesstkmDist = distanceInKm(telaviv, lessTnehkm)
      println(s"less then km $lesstkmDist")
      val dist1 = distanceInKm(batyam, holon)
      val dist2 = distanceInKm(haifa, holon)
      println(s" bat yam to holon $dist1, haifa to holon $dist2")
      val weight1 = weighing(dist1)
      val weight2 = weighing(dist2)
      println(s"Weights: $weight1, $weight2")

    }

  */
  /*
    test("Temperature interpolation") {
      val shouldGetLessthenKM = List((batyam, 31.0), (telaviv, 25.0), (batyam, 41.0))
      val prediction1 = predictTemperature(shouldGetLessthenKM, lessTnehkm)
      assert(25.0 == prediction1)
      assert(41.0 != prediction1)

      val shouldRunAgg1 = List((lessTnehkm, 31.0), (telaviv, 131.0), (batyam, 41.0), (haifa, 100.05))
      val prediction2 = predictTemperature(shouldRunAgg1, holon)
      println(prediction2)
      assert(41 === prediction2.toInt)
      val pred3 = predictTemperature(List((batyam, 41.2), (haifa, 100.05)), holon)
      val pred4 = predictTemperature(List((batyam, 100.05), (haifa, 41.2)), holon)
      println(s"$pred3 , $pred4")
      assert(41.20001509721863 === pred3)
      assert(100.04998490278136 === pred4)
    }


    test("predictTemperature: some point closer") {
      val location1 = Location(1, 1)
      val temp1 = 10d
      val location2 = Location(-10, -10)
      val temp2 = 50d
      val list = List(
        (location1, temp1),
        (location2, temp2)
      )
      val result = Visualization.predictTemperature(list, Location(0, 0))
      assert(temp1 - result < temp2 - result)
      //      [Observed Error] 50.0 did not equal 10.0 +- 1.0E-4 Incorrect predicted temperature at Location(90.0,-180.0): 50.0. Expected: 10.0
    }
  */
  test("color linear interpolation") {
    //        val points1 = List((20.0, new Color(10, 10, 10)), (50.0, new Color(20, 20, 20)))
    //        val result1 = interpolateColor(points1, 35.0)
    //        println("For 35.0 " + result1)
    //        println("For 45.0 " + interpolateColor(points1, 45.0))
    //        assert(result1 == new Color(17, 17, 17))
    //        Incorrect predicted color: Color(-2147483648,0,0). (
    val scale = List((0.0, Color(255, 0, 0)), (1.0, Color(0, 0, 255)))
    val value = 0.5
    val expected = new Color(128, 0, 128)
    val actual1 = interpolateColor(scale, value)
    println(s"actual1 $actual1")
    //        val points2 = List((7.6, new Color(5, 10, 2)), (20.0, new Color(10, 10, 10)), (37.6, new Color(50, 150, 20)), (50.0, new Color(20, 20, 20)), (87.6, new Color(50, 120, 220)))
    //        val result2 = interpolateColor(points2, 35.0)
    //        println(result2)
    //        assert(new Color(44, 129, 18) === result2)
    //   Incorrect predicted color: Color(120,120,120). Expected: Color(191,0,64) (scale = List((-1.0,Color(255,0,0)), (0.0,Color(0,0,255))), value = -0.75)
    //   Incorrect predicted color: Color(120,120,120). Expected: Color(255,0,0) (scale = List((-100.0,Color(255,0,0)), (-42.77841225324652,Color(0,0,255))), value = -100.0)
    //Expected: Color(128,0,128) (scale = List((-2.147483648E9,Color(255,0,0)), (1.0,Color(0,0,255))), value = -1.0737418235E9)

  }


  test("visualize ") {
    val colorScale = List((-50.0, new Color(255, 255, 255)))
    val temperatures1 = List((new Location(50, -100), 8.0), (new Location(30, 100), 28.0), (new Location(36, 150), 38.0))

    val result1 = visualize(temperatures1, colorScale)
    println(result1)
  }
}

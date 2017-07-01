package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

import Interaction._

import scala.collection.concurrent.TrieMap

@RunWith(classOf[JUnitRunner])
class InteractionTest extends FunSuite with Checkers {

  test("Generate tiles GPOS coordinates") {
    println(tileLocation(0, 0, 0))
    println(tileLocation(1, 0, 0))
    println(tileLocation(2, 0, 0))
    println(tileLocation(1, 1, 0))
    println(tileLocation(1, 1, 1))
    println(tileLocation(2, 1, 0))
    println(tileLocation(2, 1, 1))
    println(tileLocation(2, 3, 3))
  }
}

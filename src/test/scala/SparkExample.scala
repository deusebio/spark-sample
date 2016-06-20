package spark.sample

import defaults.{DefaultSettings, DefaultSparkContext}
import logging.Logging

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TopicClassifierTest extends FunSuite
  with BeforeAndAfterEach with BeforeAndAfterAll
  with DefaultSparkContext
  with Logging {

  // Settings
  val settings = DefaultSettings.fromArgList(Array())
  implicit val sc = makeSparkContext(settings.sparkSettings)

  val n: Int = 1000

  test("that spark correctly works") {
    val rdd = sc.parallelize(1 to n)

    assert(rdd.count() == n)

    assert(rdd.collect().size == n)

    assert(rdd.first()== 1)
  }

}

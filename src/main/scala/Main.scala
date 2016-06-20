package spark.sample

import defaults.{DefaultSettings, DefaultSparkContext, MainClass}

import logging.Logging
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

object Main
  extends MainClass with DefaultSparkContext
  with Logging {

  protected def body(settings: DefaultSettings)(implicit fs: FileSystem,
                                                sc: SparkContext): Unit = {

    val rdd = sc.parallelize(1 to 1000)

    println(" Count on RDD: " + rdd.count().toString)
    println(" Count on Array: " + rdd.collect().size)

    println(" First element: " + rdd.first())


  }

}

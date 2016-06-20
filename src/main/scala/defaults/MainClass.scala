package defaults

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

import logging.Logging

trait MainClass { this: Logging with DefaultSparkContext =>

  private lazy val hadoopConf = new Configuration() // assumes hadoop config directory is on the classpath

  // Comment to run with the usual way
  private def fileSystem(sc: SparkContext) =
   if (sc.isLocal) FileSystem.getLocal(hadoopConf)
   else FileSystem.get(hadoopConf)

  protected def body(settings: DefaultSettings)(implicit fs: FileSystem, sparkContext: SparkContext): Unit

  def main(args: Array[String]) = {
    val settings = DefaultSettings.fromArgList(args)
    log.info(s"Loaded settings - ${settings.write}")

    val sparkContext = makeSparkContext(settings.sparkSettings)

    val fs = fileSystem(sparkContext)

    log.info("Starting application")
    val startTime = System.currentTimeMillis()
    body(settings)(fs, sparkContext)

    log.info(s"Execution finished in [${(System.currentTimeMillis() - startTime) / 60000}] min(s)")
    log.info("Closing application")
    sparkContext.stop()

    log.info("Spark context terminated ... exiting")
  }

}

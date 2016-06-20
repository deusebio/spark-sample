package defaults

import org.apache.spark.{ SparkContext, SparkConf }

trait DefaultSparkContext {

  protected def appName = "Recommender"
  protected def broadcastBlockSizeMb = 50
  protected def serializerClassName = "org.apache.spark.serializer.KryoSerializer"
  protected def serializerBufferMb = 200
  protected def shuffleManager = "hash"
  protected def eventLog = false
  protected def compressLog = true
  protected def akkaFramesize = 30
  // protected def dirLog = "/tmp/spark-events"

  private def thisJar = {
    this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
  }

  private def setJars(settings: SparkSettings, sparkConf: SparkConf, jars: String*) =
    if (settings.master startsWith "local") sparkConf
    else sparkConf.setJars(thisJar +: jars)

  private def withSparkConf(settings: SparkSettings)(f: SparkConf => SparkContext) = f(
    new SparkConf()
    .setAppName(appName)
    .setMaster(settings.master)
    .set("spark.default.parallelism",      settings.numPartitions.toString)
    .set("spark.executor.instances",       settings.numExecutors.toString)
    // .set("spark.executor.memory",          settings.executorMemory)
    // .set("spark.executor.cores",           settings.numExecutorCores)
    // .set("spark.yarn.jar",                 settings.jarLocation)
    // .set("spark.driver.memory",            settings.driverMemory)
    // .set("spark.storage.memoryFraction",   settings.storageFraction.toString)
    .set("spark.broadcast.blockSize",      (broadcastBlockSizeMb * 1024).toString)
    // .set("spark.serializer",               serializerClassName)
    // .set("spark.kryoserializer.buffer.mb", serializerBufferMb.toString)
    .set("spark.eventLog.enabled",         eventLog.toString())
    .set("spark.eventLog.compress",        compressLog.toString())
    .set("spark.yarn.executor.memoryOverhead", "4096"))
    // .set("spark.akka.frameSize",           akkaFramesize.toString()))
    // .set("spark.shuffle.manager",          shuffleManager))

  protected def makeSparkContext(settings: SparkSettings) = withSparkConf(settings) { conf =>
    new SparkContext(setJars(settings, conf))
  }


}

package defaults

import java.io.PrintWriter

import scala.util.{ Failure, Success, Try }

import org.apache.commons.cli._
import org.apache.commons.cli.{ Option => CliOption }

case class DefaultSettings(sparkSettings: SparkSettings,
                       inputBase: String,
                       debug: Boolean) extends Setting {

  def write =
    s"""
       |${sparkSettings.write}
       |csp-application :
       |---------------
       |inputDir = $inputBase
       |debug = $debug
     """.stripMargin
}

object DefaultSettings {

  private def sparkMaster         = new CliOption("m", "master", true, "Url to spark master; can also be yarn-client or local[n] where n is number of threads, ideally > 1")
  private def numExecutors        = new CliOption("n", "numExecutors", true, "How many executors are requested for the spark application")
  private def executorMem         = new CliOption("e", "executorMemory", true, "How much memory is allocated to executors")
  private def numExecutorCores    = new CliOption("c", "numExecutorCores", true, "How many cores are allocated per executor")
  private def driverMem           = new CliOption("r", "driverMemory", true, "How much memory is allocated to driver")
  private def numPartitions       = new CliOption("N", "numParitions", true, "How many partitions to use by default")
  private def storageFraction     = new CliOption("f", "storageFraction", true, "What percentage of memory to allocate for storage")
  private def jarLocation         = new CliOption("j", "jarLocation", true, "Location of the Spark assembly jar: necessary when setting master to [yarn-client]")
  private def inputBase           = new CliOption("i", "inputBase", true, "Base dir setting for the input files")
  private def debugMode           = new CliOption("b", "debug", false, "Specifies whether the application is operating under debugging conditions")

  private def options = List(
    inputBase,
    debugMode).foldLeft(new Options) { _ addOption _ }

  private def makeSparkSettings(cli: CommandLine) = SparkSettings(
    cli.getOptionValue(sparkMaster.getOpt, "local[4]"),
    cli.getOptionValue(numExecutors.getOpt, "8").toInt,
    cli.getOptionValue(executorMem.getOpt, "512m"),
    cli.getOptionValue(numExecutorCores.getOpt, "1"),
    cli.getOptionValue(driverMem.getOpt, "512m"),
    cli.getOptionValue(numPartitions.getOpt, "16").toInt,
    cli.getOptionValue(storageFraction.getOpt, "0.33").toDouble,
    cli.getOptionValue(jarLocation.getOpt, "hdfs:///user/tempuser/spark/spark-assembly.jar")
  )

  private def makeSettings(cli: CommandLine) = DefaultSettings(
    makeSparkSettings(cli),
    cli.getOptionValue(inputBase.getOpt, "data-in"),
    cli.hasOption(debugMode.getOpt)
  )

  private def printHelp() {
    val sysOut = new PrintWriter(System.out)
    new HelpFormatter().printUsage(sysOut, 100, "CSP", options)
    sysOut.close()
  }

  def fromArgList(args: Array[String]) = Try {
    makeSettings { new BasicParser().parse(options, args) }
  } match {
    case Success(settings) => settings
    case Failure(e) => throw e
  }

}

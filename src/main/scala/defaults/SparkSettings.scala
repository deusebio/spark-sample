package defaults

case class SparkSettings(master: String,
                         numExecutors: Int,
                         executorMemory: String,
                         numExecutorCores: String,
                         driverMemory: String,
                         numPartitions: Int,
                         storageFraction: Double,
                         jarLocation: String
                          ) extends Setting {
  def write =
    s"""
       |spark :
       |-----
       |master = $master
       |numExecutors = $numExecutors
       |executorMemory = $executorMemory
       |executorCores = $numExecutorCores
       |driverMemory = $driverMemory
       |numPartitions = $numPartitions
       |storageFraction = $storageFraction
       |jarLocation = $jarLocation
     """.stripMargin
}

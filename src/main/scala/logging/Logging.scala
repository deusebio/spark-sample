package logging

import org.apache.log4j.Logger
import org.apache.log4j.Level

trait Logging {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  protected def log = Logger.getLogger("main")

}

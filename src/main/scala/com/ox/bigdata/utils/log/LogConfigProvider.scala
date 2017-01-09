package com.ox.bigdata.utils.log

import java.io.{File, InputStream}
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.core.joran.spi.JoranException
import ch.qos.logback.core.util.StatusPrinter
import org.slf4j.LoggerFactory

object LogConfigProvider extends LogSupport{

  def reset(filename: String, fileStream: InputStream): Unit = {

    try {
      new File(filename).exists() match {
        case true => {
          useConfig(_.doConfigure(new File(filename)))
          return
        }
        case _ => log.warn("log config is not exist in outer "+ filename)
      }
    } catch {
      case e: Exception => {
        log.warn("Log config is not exist in outer " + filename)
      }
    }

    try {
      fileStream.available() match {
        case x if(x>0) =>{
          useConfig(_.doConfigure(fileStream))
          return
        }
        case _ => printf("log config is not exist in resource")
      }
    } catch {
      case e: Exception => {
        log.warn("Log config is not exist in resource")
      }
    }
  }

  def useConfig(f: JoranConfigurator => Unit) {
    //this.getClass.getResourceAsStream(file)
    //System.setProperty("logback.configurationFile", filename)
    val context: LoggerContext = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
    try {
      val configurator: JoranConfigurator = new JoranConfigurator();
      configurator.setContext(context);
      // Call context.reset() to clear any previous configuration, e.g. default
      // configuration. For multi-step configuration, omit calling context.reset().
      context.reset();
      f(configurator)
    } catch {
      case e: JoranException =>
        log.warn("log: reset config error")
      // StatusPrinter will handle this
    }
    StatusPrinter.printInCaseOfErrorsOrWarnings(context);
  }

}


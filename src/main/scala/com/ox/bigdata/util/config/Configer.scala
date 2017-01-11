package com.ox.bigdata.util.config

import java.io.File
import com.ox.bigdata.util.log.LogSupport
import com.typesafe.config.{Config, ConfigFactory}


trait Configer extends LogSupport {

  def appname: String

  var config: Config = null

  //get path from envrioment to relative path
  protected var configHome = s"../$appname-conf/"

  /**
   * set the config file, file names without path
   *
   * get files from configHome, files in resources dir will be load too.
   * note：the key values load in the first file will cover the key values load after
   *
   * @param files
   */

  def setConfigFiles(files: String*): Unit = {
    //log.debug(s"config home: $configHome")
    config = files.toList.map(load).reduce((a, b) => a.withFallback(b))
  }

  protected def load(file: String): Config = {
    val resourceFile = file
    val configFile = new File(makePath(file))
    if (configFile.exists()) {
      //log.debug(s"loading file[${configFile.getPath}] and resource[$resourceFile]")
      ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load(resourceFile))
    }
    else {
      //log.debug(s"loading resource[$resourceFile]")
      ConfigFactory.load(resourceFile)
    }
  }

  protected def makePath(filename: String): String = {
    val newDir = configHome.trim.replaceAll( """\\""", "/")
    if (newDir.endsWith("/")) configHome + filename
    else configHome + "/" + filename
  }

}

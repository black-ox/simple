package com.ox.bigdata.utils.config

import java.io.File
import com.ox.bigdata.utils.log.LogSupport
import com.typesafe.config.{Config, ConfigFactory}


trait Configer extends LogSupport {

  def appname: String

  var config: Config = null

  //get path from envrioment to relative path
  protected var configHome = s"../$appname-conf/"

  /**
   * 设置配置文件，只写文件名即可。
   *
   * 默认从configHome路径去读文件，同时如果resources下有同名文件也会读取
   * 注意：在前面的文件会覆盖后面的文件的相同key的配置值
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

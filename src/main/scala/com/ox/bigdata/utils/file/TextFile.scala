package com.ox.bigdata.utils.file

import java.io.{File, FileOutputStream, OutputStreamWriter}

import com.ox.bigdata.utils.Using

class TextFile(filename: String) extends Using {

  def write(data: List[List[String]]): Unit = {
    val strData = data.map(x => x.mkString(",") + ",").mkString("\n") + "\n"
    writeToFile(filename, strData)
  }

  def write(data: String): Unit = {
    writeToFile(filename, data)
  }

  def delete(): Unit = {
    new File(filename) delete()
  }

  private def writeToFile(fileName: String, data: String): Unit = {
    using(new OutputStreamWriter(new FileOutputStream(fileName),"UTF-8")) {
      fileWriter => fileWriter.write(data)
    }
  }
}

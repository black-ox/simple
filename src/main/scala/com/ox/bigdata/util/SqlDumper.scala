package com.ox.bigdata.util

import java.nio.file.FileAlreadyExistsException

import com.ox.bigdata.util.file.TextFile

trait SqlDumper {

  protected def systemIndex: Int = 6
  protected def moduleIndex: Int = 7
  protected def outputPath: String = "./script/service/"
  private var fileList: List[String] = List()

  def dump(name: String, sql: String, description: String = "") {
    val filePath = getFilePath(name)

    // sanity check
    if (fileList.exists(_.equalsIgnoreCase(filePath))) throw new FileAlreadyExistsException(filePath)

    // bump to file
    val file = new TextFile(filePath)
    file.write(generateText(name, sql, description))

    // remember this file
    fileList = filePath :: fileList
  }

  private def generateText(name: String, sql: String, description: String): String = {
    val notes = s"/* Name: $name\n   File: ${generateFileName(name)}\n   Note: $description\n */\n"
    notes + sql
  }

  private def getFilePath(name: String): String = {
    outputPath + generateFileName(name) + ".sql"
  }

  private def generateFileName(name: String): String = {
    val names = this.getClass.getName.split("\\.").toList
    val system = names(systemIndex).toLowerCase
    val module = names(moduleIndex).toLowerCase
    val clazz = names.last.toLowerCase
    val newName = name.replaceAll("\\\\|/|:|\\*|\\?|\"|<|>|\\|| ", "").toLowerCase //remove all invalid chars
    s"$system.$module.$clazz.$newName"
  }

}

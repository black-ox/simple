package com.ox.bigdata.util.file

import java.io.FileReader

import au.com.bytecode.opencsv.CSVReader

import scala.collection.JavaConversions._

trait StringTableReader {
  def read(): List[String]

  def readAll(): List[List[String]]

  //def writeAll(rows:Iterator[List[String]]):Unit
  def close(): Unit
}

object StringTableReader {
  def csvReader(importFile: String, sep: Char, skip: Int): StringTableReader = new OpenCSVReader(importFile, sep, skip)
}

private[this] class OpenCSVReader(importFile: String, sep: Char, skip: Int) extends StringTableReader {
  val csvReader = new CSVReader(new FileReader(importFile), sep, '\"', skip)

  def read(): List[String] = csvReader.readNext().toList

  def readAll(): List[List[String]] = csvReader.readAll().toList.map(_.toList)

  //def writeAll(rows:Iterator[List[String]]):Unit = csvWriter.writeAll(rows.map(_.toArray).toList)
  def close(): Unit = csvReader.close()
}


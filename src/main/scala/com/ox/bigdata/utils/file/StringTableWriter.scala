package com.ox.bigdata.utils.file

import java.io.Writer
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConversions.seqAsJavaList

trait StringTableWriter {
  def write(strings: List[String]): Unit

  def writeAll(rows: List[List[String]]): Unit

  def writeAll(rows: Iterator[List[String]]): Unit

  def close(): Unit
}

object StringTableWriter {
  def csvWriter(writer: Writer, separator: Char = ',', lineEnd: String = "\n"): StringTableWriter = new OpenCSVWriter(writer, separator, lineEnd)
}

private[this] class OpenCSVWriter(writer: Writer, separator: Char, lineEnd: String) extends StringTableWriter {
  val csvWriter = new CSVWriter(writer, separator, CSVWriter.NO_QUOTE_CHARACTER, lineEnd)

  def write(strings: List[String]): Unit = csvWriter.writeNext(strings.toArray)

  def writeAll(rows: List[List[String]]): Unit = csvWriter.writeAll(rows.map(_.toArray))

  def writeAll(rows: Iterator[List[String]]): Unit = csvWriter.writeAll(rows.map(_.toArray).toList)

  def close(): Unit = writer.close()
}


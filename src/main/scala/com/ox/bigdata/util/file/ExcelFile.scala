package com.ox.bigdata.util.file

import java.io.{File, FileInputStream, FileOutputStream}

import com.ox.bigdata.util.Using
import com.ox.bigdata.util.log.LogSupport
import org.apache.poi.ss.usermodel.{CellStyle, IndexedColors}
import org.apache.poi.xssf.streaming.{SXSSFSheet, SXSSFWorkbook}
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}


class ExcelFile(filename: String) extends LogSupport with Using {

  protected val randomAccessWindowSize = 1000

  var stringColNames: List[String] = Nil

  /**
   * @param titles
   * @param contents
   */

  protected def useExcel(templateFile: String)(f: SXSSFWorkbook => Unit): Unit = {
    //log.debug(s"write excel file [$filename], WindowSize = $randomAccessWindowSize")
    val inputStream = this.getClass.getResourceAsStream(templateFile)
    val workbookTemplate = new XSSFWorkbook(inputStream)
    inputStream.close()
    val workbook = new SXSSFWorkbook(workbookTemplate)
    workbook.setCompressTempFiles(true)

    f(workbook)

    using(new FileOutputStream(filename)) {
      outputFile => workbook.write(outputFile)
    }
  }


  def write(titles: List[String], contents: List[List[String]]): Unit = {
    useExcel("/Excel2007Template.xlsx") {
      workbook =>
        // write the titles
        val sheet = workbook.getSheetAt(0).asInstanceOf[SXSSFSheet]
        sheet.setRandomAccessWindowSize(randomAccessWindowSize)
        val titleRow = sheet.createRow(0)
        titles.zipWithIndex.foreach {
          case (title, index) =>
            val cell = titleRow.createCell(index)
            cell.setCellValue(title)
            cell.setCellStyle(getTitleStyle(workbook))
        }

        // write contents
        contents.zipWithIndex.foreach {
          case (content, index) =>
            generateRow(sheet, index + 1, content, titles, stringColNames, getContextStyle(workbook))
        }
    }
  }

  /**
   * delete excel file
   */
  def delete(): Unit = {
    new File(filename).delete()
    //log.debug(s"delete excel file [$filename]. isSuccess = $result")
  }

  /**
   * read excel into a StringTable
   *
   * @return excel content
   */
  def read(): List[List[String]] = {
    val file = new File(filename)
    if (!file.exists()) return Nil

    val workbook = new XSSFWorkbook(new FileInputStream(file))
    val sheet = workbook.getSheetAt(0)
    val columnNum = sheet.getRow(0).getPhysicalNumberOfCells
    (0 to sheet.getLastRowNum).map(i => getRowContents(sheet, i, columnNum)).toList

  }

  def generateRow(sheetS: SXSSFSheet, rowId: Int, values: List[String], name: List[String], stringColNames: List[String], style: CellStyle): Unit = {
    val row = sheetS.createRow(rowId)
    row.setHeight(255)

    values.zipWithIndex.foreach {
      x =>
        val cell = row.createCell(x._2)
        cell.setCellStyle(style)
        try {
          //if(stringColIndices(name,stringColNames).contains(x._2))
          cell.setCellValue(x._1.toString)
          //          else
          //          cell.setCellValue(x._1.toDouble)
        }
        catch {
          case _: Throwable => cell.setCellValue(x._1)
        }
    }
  }

  protected def getTitleStyle(workbook: SXSSFWorkbook): CellStyle = {
    val style = workbook.createCellStyle()
    val font = workbook.createFont()
    font.setFontName("Arial")
    font.setFontHeightInPoints(10)
    style.setFont(font)
    style.setFillForegroundColor(IndexedColors.TURQUOISE.getIndex)
    style.setVerticalAlignment(CellStyle.VERTICAL_CENTER)
    style.setWrapText(true)
    style.setFillPattern(CellStyle.SOLID_FOREGROUND)
    style.setBorderBottom(CellStyle.BORDER_THIN)
    style.setBorderLeft(CellStyle.BORDER_THIN)
    style.setBorderRight(CellStyle.BORDER_THIN)
    style.setBorderTop(CellStyle.BORDER_THIN)
    style
  }

  protected def getContextStyle(workbook: SXSSFWorkbook): CellStyle = {
    val style = workbook.createCellStyle()
    val font = workbook.createFont()
    font.setFontName("Arial")
    font.setFontHeightInPoints(10)
    style.setFont(font)
    style
  }

  protected def getRowContents(sheet: XSSFSheet, rowId: Int, columnNum: Int): List[String] = {
    (0 until columnNum).map(colId => Option(sheet.getRow(rowId).getCell(colId)).getOrElse("").toString).toList
  }

  /**
   * transform excel tables into List
 *
   * @param fileName
   * @return List[ List[String] ]
   */

  def analysisExcle(fileName: String): List[List[String]] = {
    var list: List[List[String]] = List[List[String]]()
    val file = new File(fileName)
    if (file.exists()) {
      val input = new FileInputStream(file)
      val wb_template = new XSSFWorkbook(input)
      val sheet0 = wb_template.getSheetAt(0)
      val last: Int = sheet0.getLastRowNum
      for (i <- 0 to last) {
        var row: List[String] = List[String]()
        val iterator = sheet0.getRow(i).cellIterator()
        while (iterator.hasNext) {
          val cell = iterator.next()
          if (cell.toString != "") {
            row = row :+ cell.toString
          }
        }
        list = list :+ row
      }
    }
    list
  }


}
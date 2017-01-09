package com.ox.bigdata.utils.file

import java.io.{File, FileInputStream, FileOutputStream}

import com.ox.bigdata.utils.Using
import com.ox.bigdata.utils.log.LogSupport
import org.apache.poi.ss.usermodel.{CellStyle, IndexedColors}
import org.apache.poi.xssf.streaming.{SXSSFSheet, SXSSFWorkbook}
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}


class ExcelFile(filename: String) extends LogSupport with Using {

  protected val randomAccessWindowSize = 1000

  var stringColNames: List[String] = Nil

  /**
   * 将标题行和内容写入excel文件中
   *
   * @param titles 标题
   * @param contents 所有内容
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
   * 删除excel文件
   */
  def delete(): Unit = {
    val result = new File(filename).delete()
    //log.debug(s"delete excel file [$filename]. isSuccess = $result")
  }

  /**
   * 将excel文件内容读入到一个StringTable中
   *
   * @return excel 文件内容
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
   * 将excle 表格解析成List 集合
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
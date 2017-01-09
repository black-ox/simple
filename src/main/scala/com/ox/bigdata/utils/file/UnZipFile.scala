package com.ox.bigdata.utils.file

import java.io._
import java.util.zip.GZIPInputStream

import com.ox.bigdata.utils.Using
import com.ox.bigdata.utils.log.LogSupport
import org.apache.tools.tar.TarInputStream
import org.apache.tools.zip.ZipFile

import scala.collection.JavaConversions

trait UnZipFile extends LogSupport with Using {
  private final val FILE_EXT_TAR_GZ: String = ".tar.gz"
  private final val FILE_EXT_GZ: String = ".gz"
  private final val FILE_EXT_TAR: String = ".tar"
  private final val FILE_EXT_ZIP: String = ".zip"
  private var FILE_BUF_SIZE: Int = 2048

  /**
   * 解压.zip文件
   *
   * @param src       源文件
   * @param dstPath   目标文件夹
   * @param fNameKeys 需要解压的文件名关键字集合
   * @return 解压文件路径集合,key-文件关键字，contents-对应文件
   */
  def unzipZip(src: File, dstPath: File, fNameKeys: List[String]): Option[Map[String, File]] = {
    //检查文件是否存在
    if (!src.isFile) {
      log.error("Source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    //检查文件是否为zip类型
    if (!src.getName.toLowerCase.endsWith(FILE_EXT_ZIP)) {
      log.error("Source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    //创建临时文件
    if (!FileTools.CreateFolder(dstPath)) {
      log.error("Failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

    val zip: ZipFile = new ZipFile(src)
    val entries = JavaConversions.enumerationAsScalaIterator(zip.getEntries).toList

    //提取文件
    val result = entries.filter(x => {
      val name = new File(x.getName).getName
      (!x.isDirectory) && (getMatchKey(name, fNameKeys)).nonEmpty
    }).map(x => {
      val zipEntry = new File(x.getName)
      val key = getMatchKey(zipEntry.getName, fNameKeys).get

      //创建该文件必需的父文件夹
      val tgtFile = new File(dstPath, zipEntry.getPath())
      val parentFolder = tgtFile.getParentFile()
      if (!FileTools.CreateFolderWithParent(parentFolder)) {
        log.error("Failure happpen when create output directory:" + parentFolder.getAbsolutePath())
        return None
      }

      // 解压文件
      using(zip.getInputStream(x)) {
        in =>
          using(new FileOutputStream(tgtFile)) {
            out =>
              if (!FileTools.CopyFile(in, out)) {
                log.error("Failed to unzip file " + zipEntry.getAbsolutePath + " from " + src)
                return None
              }
          }
      }

      (key, tgtFile)
    }).toMap

    zip.close()

    Some(result)
  }

  /**
   * 解压tar文件
   *
   * @param src       源文件
   * @param dstPath       目标文件夹
   * @param fNameKeys 需要解压的文件名关键字集合
   * @return 解压文件路径集合
   */

  private def unzipTar(src: File, dstPath: File, fNameKeys: List[String]): Option[Map[String, File]] = {
    //检查文件是否存在
    if (!src.isFile) {
      log.error("Source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    //检查文件是否为zip类型
    if (!src.getName.toLowerCase.endsWith(FILE_EXT_TAR)) {
      log.error("Source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    //创建临时文件
    if (!FileTools.CreateFolder(dstPath)) {
      log.error("Failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

    //解压文件
    var unzipFiles: Option[Map[String, File]] = None
    using(new TarInputStream(new FileInputStream(src))) {
      tarIn => {
        unzipFiles = processEntry(tarIn, fNameKeys, dstPath)
      }
    }
    return unzipFiles
  }

  /**
   * 解压tar.gz文件
   *
   * @param src       源文件
   * @param dstPath       目标文件夹
   * @param fNameKeys 需要解压的文件名关键字集合
   * @return 解压文件路径集合
   */

  def unzipTarGz(src: File, dstPath: File, fNameKeys: List[String]): Option[Map[String, File]] = {
    //检查文件是否存在
    if (!src.isFile) {
      log.error("Source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    //检查文件是否为zip类型
    if (!src.getName.toLowerCase.endsWith(FILE_EXT_TAR_GZ)) {
      log.error("Source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    //创建临时文件
    if (!FileTools.CreateFolder(dstPath)) {
      log.error("Failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

    //解压文件
    var unzipFiles: Option[Map[String, File]] = None
    using(new TarInputStream(new GZIPInputStream(new FileInputStream(src)))) {
      tarIn => {
        unzipFiles = processEntry(tarIn, fNameKeys, dstPath)
      }
    }
    return unzipFiles
  }

  /**
   * 解压gz文件
   *
   * @param src       源文件
   * @param dstPath       目标文件夹
   * @return 解压文件路径
   */
  def unzipGZ(src: File, dstPath: File): Option[File] = {
    //检查文件是否存在
    if (!src.isFile) {
      log.error("Source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    //检查文件是否为zip类型
    if (!src.getName.toLowerCase.endsWith(FILE_EXT_GZ)) {
      log.error("Source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    //创建临时文件
    if (!FileTools.CreateFolder(dstPath)) {
      log.error("Failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

    //解压文件
    var unzipFile: Option[File] = None
    using(new GZIPInputStream(new FileInputStream(src))) {
      gzIn => {
        val dstFile = new File(dstPath, src.getName.substring(0, src.getName.lastIndexOf(".")))
        using(new FileOutputStream(dstFile)) {
          out =>
            if (FileTools.CopyFile(gzIn, out)) {
              unzipFile = Some(dstFile)
            }
        }
      }
    }
    unzipFile
  }

  /**
   * 解压tar文件
   * @param tarIn tar输入流
   * @param keys 文件关键字
   * @param tgtFolder 目标路径
   * @return key-解压文件,contents-解压文件路径
   */

  private def processEntry(tarIn: TarInputStream, keys: List[String], tgtFolder: File): Option[Map[String, File]] = {
    val entry = tarIn.getNextEntry

    if (entry == null) {
      Some(Map[String, File]())
    }else {
      val zipEntry = new File(entry.getName)
      val key = getMatchKey(zipEntry.getName, keys)

      //文件夹或不包含key的文件直接跳过
      if (!(zipEntry.isFile && key.nonEmpty)) {
        processEntry(tarIn, keys, tgtFolder)
      }
      else {
        //创建该文件必需的父文件夹
        val tgtFile = new File(tgtFolder, zipEntry.getPath())
        val parentFolder = tgtFile.getParentFile()
        if (!FileTools.CreateFolderWithParent(parentFolder)) {
          log.error("Failure happpen when create output directory:" + parentFolder.getAbsolutePath())
          None
        }else{
          // 解压文件
          var untarSuccess = true
          using(new FileOutputStream(tgtFile)) {
            out =>
              if (!FileTools.CopyFile(tarIn, out)) {
                log.error("Failed to unzip file " + zipEntry.getAbsolutePath)
                untarSuccess = false
              }
          }
          if (!untarSuccess)
            None
          else {
            val succeedResults = processEntry(tarIn, keys, tgtFolder)
            if (succeedResults.isEmpty)
              None
            else
              Some(succeedResults.get + (key.get -> tgtFile))
          }
        }
      }
    }

  }


  /**
   * 获取文件名匹配的主键
   * @param filename 文件名
   * @param fileKeys 待匹配主键
   * @return 匹配的主键
   */
  def getMatchKey(filename: String, fileKeys: List[String]): Option[String] = {
    fileKeys.find(x => x.r.findFirstIn(filename).nonEmpty)
  }

}

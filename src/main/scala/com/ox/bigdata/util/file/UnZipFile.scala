package com.ox.bigdata.util.file

import java.io._
import java.util.zip.GZIPInputStream

import com.ox.bigdata.util.Using
import com.ox.bigdata.util.log.LogSupport
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
    * @param src
    * @param dstPath
    * @param fNameKeys
    * @return file path unzip, key-file，contents-file
    */
  def unzipZip(src: File, dstPath: File, fNameKeys: List[String]): Option[Map[String, File]] = {
    if (!src.isFile) {
      LOG.error("Source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    if (!src.getName.toLowerCase.endsWith(FILE_EXT_ZIP)) {
      LOG.error("Source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    if (!FileTools.CreateFolder(dstPath)) {
      LOG.error("Failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

    val zip: ZipFile = new ZipFile(src)
    val entries = JavaConversions.enumerationAsScalaIterator(zip.getEntries).toList

    val result = entries.filter(x => {
      val name = new File(x.getName).getName
      (!x.isDirectory) && getMatchKey(name, fNameKeys).nonEmpty
    }).map(x => {
      val zipEntry = new File(x.getName)
      val key = getMatchKey(zipEntry.getName, fNameKeys).get

      val tgtFile = new File(dstPath, zipEntry.getPath())
      val parentFolder = tgtFile.getParentFile()
      if (!FileTools.CreateFolderWithParent(parentFolder)) {
        LOG.error("failed to create output directory:" + parentFolder.getAbsolutePath())
        return None
      }

      using(zip.getInputStream(x)) {
        in =>
          using(new FileOutputStream(tgtFile)) {
            out =>
              if (!FileTools.CopyFile(in, out)) {
                LOG.error("Failed to unzip file " + zipEntry.getAbsolutePath + " from " + src)
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
    * tar file
    *
    * @param src
    * @param dstPath
    * @param fNameKeys
    * @return path set
    */

  private def unzipTar(src: File, dstPath: File, fNameKeys: List[String]): Option[Map[String, File]] = {
    if (!src.isFile) {
      LOG.error("source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    if (!src.getName.toLowerCase.endsWith(FILE_EXT_TAR)) {
      LOG.error("source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    if (!FileTools.CreateFolder(dstPath)) {
      LOG.error("failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

    var unzipFiles: Option[Map[String, File]] = None
    using(new TarInputStream(new FileInputStream(src))) {
      tarIn => {
        unzipFiles = processEntry(tarIn, fNameKeys, dstPath)
      }
    }
    return unzipFiles
  }

  /**
    * tar.gz file
    *
    * @param src
    * @param dstPath
    * @param fNameKeys
    * @return paths set
    */

  def unzipTarGz(src: File, dstPath: File, fNameKeys: List[String]): Option[Map[String, File]] = {
    if (!src.isFile) {
      LOG.error("source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    if (!src.getName.toLowerCase.endsWith(FILE_EXT_TAR_GZ)) {
      LOG.error("source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    if (!FileTools.CreateFolder(dstPath)) {
      LOG.error("failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

    var unzipFiles: Option[Map[String, File]] = None
    using(new TarInputStream(new GZIPInputStream(new FileInputStream(src)))) {
      tarIn => {
        unzipFiles = processEntry(tarIn, fNameKeys, dstPath)
      }
    }
    return unzipFiles
  }

  /**
    * gz file
    *
    * @param src
    * @param dstPath
    * @return paths set
    */
  def unzipGZ(src: File, dstPath: File): Option[File] = {
    if (!src.isFile) {
      LOG.error("source file " + src.getAbsolutePath + " is not a file.")
      return None
    }

    if (!src.getName.toLowerCase.endsWith(FILE_EXT_GZ)) {
      LOG.error("source file " + src.getAbsolutePath + " is not a zip file.")
      return None
    }

    if (!FileTools.CreateFolder(dstPath)) {
      LOG.error("failed to create folder " + dstPath.getAbsolutePath)
      return None
    }

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
    * tar file
    *
    * @param tarIn     tar inputStream
    * @param keys
    * @param tgtFolder
    * @return key-file,contents-filepath
    */

  private def processEntry(tarIn: TarInputStream, keys: List[String], tgtFolder: File): Option[Map[String, File]] = {
    val entry = tarIn.getNextEntry

    if (entry == null) {
      Some(Map[String, File]())
    } else {
      val zipEntry = new File(entry.getName)
      val key = getMatchKey(zipEntry.getName, keys)

      if (!(zipEntry.isFile && key.nonEmpty)) {
        processEntry(tarIn, keys, tgtFolder)
      }
      else {
        val tgtFile = new File(tgtFolder, zipEntry.getPath)
        val parentFolder = tgtFile.getParentFile
        if (!FileTools.CreateFolderWithParent(parentFolder)) {
          LOG.error("failed to create output directory:" + parentFolder.getAbsolutePath)
          None
        } else {
          var untarSuccess = true
          using(new FileOutputStream(tgtFile)) {
            out =>
              if (!FileTools.CopyFile(tarIn, out)) {
                LOG.error("Failed to unzip file " + zipEntry.getAbsolutePath)
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
    * get the Primary key match the file name
    *
    * @param filename
    * @param fileKeys
    * @return Primary key
    */
  def getMatchKey(filename: String, fileKeys: List[String]): Option[String] = {
    fileKeys.find(x => x.r.findFirstIn(filename).nonEmpty)
  }

}

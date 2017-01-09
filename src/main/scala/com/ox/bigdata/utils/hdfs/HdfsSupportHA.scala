package com.ox.bigdata.utils.hdfs

import java.io._
import com.ox.bigdata.utils.log.LogSupport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

trait HdfsSupportHA extends LogSupport {

  def hdfsConf: Configuration

  def usingHdfs(errMsg: String)(f: FileSystem => Unit): Unit = {
    var hdfs: FileSystem = null

    try {
      hdfs = FileSystem.newInstance(hdfsConf)
      f(hdfs)
    }
    finally {
      try {
        if (hdfs != null) {
          hdfs.close()
        }
      } catch {
        case e: IOException =>
          log.error(errMsg)
          log.error(s"${e.getStackTrace}")
        case _: Exception => log.error(errMsg)
      }
    }
  }

  def upload(source: String, target: String): Unit = {
    usingHdfs("upload failed.") {
      hdfs =>
        val in = new BufferedInputStream(this.getClass.getResourceAsStream(source))
        val out = hdfs.create(new Path(target))
        try {
          IOUtils.copyBytes(in, out, 4096, true)
        }
        finally {
          if (in != null) in.close()
        }
    }
  }

  def delete(target: String): Unit = {
    usingHdfs("delete failed.") {
      hdfs =>
        val path = new Path(target)
        if (hdfs.exists(path))
          hdfs.delete(path, true)
    }
  }

  def emptyDir(dir: String): Unit = {
    usingHdfs("emptyDir failed.") {
      hdfs =>
        val p = new Path(dir)
        if (hdfs.isDirectory(p)) {
          val files = hdfs.listFiles(p, false)
          while (files.hasNext) {
            hdfs.delete(files.next().getPath, true)
          }
        }
    }
  }

  def download(source: String, target: String): Unit = {
    usingHdfs("download failed.") {
      hdfs =>
        val srcPath = source
        val out = new FileOutputStream(s"$target")
        val files = hdfs.listFiles(new Path(srcPath), false)
        try {
          while (files.hasNext) {
            val file = files.next()
            if (file.getPath.toString.contains("part-")) {
              val in = hdfs.open(new Path(file.getPath.toString))
              IOUtils.copyBytes(in, out, 4096, false)
            }
          }
        } finally {
          if (out != null) out.close()
        }
    }
  }

  def downloadEx(source: String, target: String): Unit = {
    usingHdfs("download failed.") {
      hdfs =>
        val srcPath = source
        val out = new FileOutputStream(s"$target")
        val files = hdfs.listFiles(new Path(srcPath), false)
        try {
          while (files.hasNext) {
            val file = files.next()
            val in = hdfs.open(new Path(file.getPath.toString))
            IOUtils.copyBytes(in, out, 4096, false)
          }
        } finally {
          if (out != null) out.close()
        }
    }
  }

  def write(source: String, target: String): Unit = {
    usingHdfs("write failed.") {
      hdfs =>
        val in = new ByteArrayInputStream(source.getBytes("UTF-8"))
        val out = hdfs.create(new Path(target))
        IOUtils.copyBytes(in, out, source.length, true)
    }
  }

  def append(source: String, target: String): Unit = {
    usingHdfs("append failed") {
      hdfs =>
        val in = new ByteArrayInputStream(source.getBytes("UTF-8"))
        val out = if (hdfs.exists(new Path(target))) {
          hdfs.append(new Path(target))
        }
        else {
          hdfs.create(new Path(target))
        }
        IOUtils.copyBytes(in, out, source.length, true)
    }
  }

  def listFiles(path: String): List[String] = {
    var result: List[String] = Nil
    usingHdfs("listFiles failed.") {
      hdfs =>
        val hdfsPath = new Path(path)
        if (hdfs.exists(hdfsPath)) {
          result = hdfs.listStatus(hdfsPath).map(_.getPath.getName).toList
          result.foreach(log.debug)
        }
    }
    result
  }

}

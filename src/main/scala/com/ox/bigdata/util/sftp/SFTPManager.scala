package com.ox.bigdata.util.sftp

import java.io._
import com.ox.bigdata.util.ftp.FtpManager
import com.ox.bigdata.util.log.LogSupport
import com.jcraft.jsch.ChannelSftp


class SFTPManager(server: String, port: String, user: String, password: String) extends
  FtpManager(server: String, port: String, user: String, password: String) with LogSupport {

  protected def getChannel(): SFTPChannel = {
    new SFTPChannel()
  }

  protected def getChannelSFTP(channel: SFTPChannel): ChannelSftp = {
    val sftpDetails: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    sftpDetails.put(SFTPConstants.SFTP_REQ_HOST, server)
    sftpDetails.put(SFTPConstants.SFTP_REQ_USERNAME, user)
    sftpDetails.put(SFTPConstants.SFTP_REQ_PASSWORD, password)
    sftpDetails.put(SFTPConstants.SFTP_REQ_PORT, port)
    channel.getChannel(sftpDetails, 60000)
  }

  protected def usingSFTP(op: ChannelSftp => Unit): Unit = {
    val channel: SFTPChannel = getChannel()
    val channelSftp: ChannelSftp = getChannelSFTP(channel)
    try {
      op(channelSftp)
    } catch {
      case e: Exception => LOG.error("SFTP actions failed ! :" + e.printStackTrace())
    } finally {
      channelSftp.quit()
      channel.closeChannel()
    }
  }

  // test failed
  override def listFiles(parentPath: String): Array[String] = {
    var res: Array[String] = Nil.toArray
    usingSFTP {
      channelSftp =>
        val result = channelSftp.ls(parentPath).toArray
        val files = result.filterNot(x => x.toString.startsWith("d"))
        res = files.toList.map(x => x.toString.split(" ").last).toArray
    }
    res
  }

  override def listDirectories(parentPath: String): Array[String] = {
    var res: Array[String] = Nil.toArray
    usingSFTP {
      ChannelSFTP =>
        val result = ChannelSFTP.ls(parentPath).toArray
        val directory = result.filter(x => x.toString.startsWith("d"))
        val folderstemp = directory.toList.map(x => x.toString.split(" ").last)
        res = folderstemp.filterNot(x => x == "." || x == "..").toArray
    }
    res
  }

  //upload file ok
  override def upload(local: String, remote: String): Boolean = {
    var res = false
    usingSFTP {
      channelSftp =>
        MakeRemoteDirectory(channelSftp, remote)
        channelSftp.put(local, remote, ChannelSftp.OVERWRITE)
        res = true
    }
    res
  }

  override def upload(localStream: InputStream, remote: String): Boolean = {
    var res = false
    usingSFTP {
      channelSFTP =>
        MakeRemoteDirectory(channelSFTP, remote)
        channelSFTP.put(localStream, remote, ChannelSftp.OVERWRITE)
        res = true
    }
    res
  }

  //download file ok
  override def download(src: String, dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Boolean = {
    var res = false
    usingSFTP {
      channelSFTP =>
        if (fileIsExists(channelSFTP, src)) {
          var index = src.lastIndexOf('/')
          if (index == -1) index = src.lastIndexOf('\\')
          val fileName = src.substring(index + 1, src.length)
          val localFile = new File(dst + "/" + fileName)
          if (!localFile.getParentFile.exists)
            localFile.getParentFile.mkdirs
          val outputStream = new FileOutputStream(localFile)
          channelSFTP.get(src, outputStream)
          outputStream.close()
          res = true
        }
    }
    res
  }

  override def downloadFiles(src: List[String], dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Unit = {
    usingSFTP {
      channelSFTP =>
        for (elem <- src) {
          if (fileIsExists(channelSFTP, elem)) {
            var index = elem.lastIndexOf('/')
            if (index == -1) index = elem.lastIndexOf('\\')
            val fileName = elem.substring(index + 1, elem.length)
            val localFile = new File(dst + "/" + fileName)
            if (!localFile.getParentFile.exists)
              localFile.getParentFile.mkdirs
            val os = new FileOutputStream(localFile)
            channelSFTP.get(elem, os)
            os.close()
          }
        }
    }
  }

  override def deleteDirectory(remote: String): Boolean = {
    var isDeleteDirectorySuccess = false
    usingSFTP {
      channelSFTP =>
        val remoteWithoutPoint: String = remote.take(1) match {
          case "." => remote.drop(1)
          case _ => remote
        }
        isDeleteDirectorySuccess = deleteDirectory(channelSFTP, remoteWithoutPoint)
    }
    isDeleteDirectorySuccess
  }

  //delete directory test ok
  private def deleteDirectory(ChannelSFTP: ChannelSftp, remoteWithoutPoint: String): Boolean = {
    try {
      val result = ChannelSFTP.ls(remoteWithoutPoint).toArray
      val (dirlist, filelist) = result.partition(x => x.toString.startsWith("d"))
      val folderstemp = dirlist.toList.map(x => x.toString.split(" ").last)
      val folders = folderstemp.filterNot(x => x == "." || x == "..")
      val filenames = filelist.toList.map(x => x.toString.split(" ").last)
      if (filenames.nonEmpty)
        filenames.foreach(f => ChannelSFTP.rm(s"/$remoteWithoutPoint/$f"))
      if (folders.isEmpty)
        ChannelSFTP.rmdir(s"/$remoteWithoutPoint")
      else {
        folders.foreach(f => {
          val filePath = s"$remoteWithoutPoint/$f"
          deleteDirectory(ChannelSFTP, filePath)
        })
        ChannelSFTP.rmdir(remoteWithoutPoint)
      }
      true
    }
    catch {
      case _: Exception =>
        //       LOG.debug("delete Directory exception!")
        false
    }
  }

  //delete foldor ok
  def deletefolder(folder: String): Boolean = {
    var res = false
    usingSFTP {
      channelSFTP =>
        channelSFTP.rmdir(folder)
        res = true
    }
    res
  }

  //delete file ok
  override def delete(pathname: String): Boolean = {
    var res = false
    usingSFTP {
      channelSFTP =>
        if (fileIsExists(channelSFTP, pathname)) {
          channelSFTP.rm(pathname)
          res = true
        }
    }
    res
  }

  override def downloadByExt(srcDir: String, baseDstDir: String, ext: String): Boolean = {
    var res = false
    usingSFTP {
      channelSFTP =>
        downloadByExt(channelSFTP, srcDir, baseDstDir, ext)
        res = true
    }
    res
  }

  private def downloadByExt(ChannelSFTP: ChannelSftp, srcDir: String, baseDstDir: String, ext: String): Unit = {
    try {
      val result = ChannelSFTP.ls(srcDir).toArray
      val direction = result.partition(x => x.toString.startsWith("d"))
      val folderstemp = direction._1.toList.map(x => x.toString.split(" ").last)
      val folders = folderstemp.filterNot(x => x == "." || x == "..")
      val filenames = direction._2.toList.map(x => x.toString.split(" ").last)

      for (file <- filenames) {
        if (file.endsWith(ext))
          ChannelSFTP.get(s"$srcDir/$file", s"$baseDstDir/$file")
      }
      folders.foreach(x => downloadByExt(ChannelSFTP, s"$srcDir/$x", s"$baseDstDir/$x", ext))
    }
    catch {
      case _: Exception =>
      //        LOG.debug("downloadByExt file exception!")
    }
  }

  /**
    * download all the files in the given path and subpath with the same ext
    * relativePath
    * srcDir
    * baseDstDir (must be abslute path)
    * ext [file ext]
    */
  private def MakeRemoteDirectory(ChannelSFTP: ChannelSftp, remote: String): String = {
    try {
      def remotepathVerified(path: String): String = path.take(1) match {
        case "." => remotepathVerified(path.drop(1))
        case "/" => path.drop(1)
        case _ => path
      }
      var checkedRemotePath = remotepathVerified(remote)
      val directories = checkedRemotePath.split('/')
      val folder = directories.head.toString
      val result = ChannelSFTP.ls(s"/$folder").toArray
      val direction = result.filter(x => x.toString.startsWith("d"))
      if (directories.length > 2)
        if (!direction.toList.map(x => x.toString.split(" ").last).contains(directories(1).toString)) {
          ChannelSFTP.cd(folder)
          ChannelSFTP.mkdir(directories(1))
          checkedRemotePath = directories(1)
        }
      checkedRemotePath
    }
    catch {
      case _: Exception =>
        //       LOG.debug("downloadByExt file exception!")
        ""
    }
  }

  private def fileIsExists(channelSFTP: ChannelSftp, file: String): Boolean = {
    try {
      var index = file.lastIndexOf('/')
      if (index == -1) index = file.lastIndexOf('\\')
      val parentPath = file.substring(0, index)
      val filename = file.substring(index + 1, file.length)
      val result = channelSFTP.ls(parentPath).toArray
      val files = result.filterNot(x => x.toString.startsWith("d"))
      val filenames = files.toList.map(x => x.toString.split(" ").last)
      filenames.contains(filename)
    }
    catch {
      case _: Exception =>
        //       LOG.debug("check file exists exception!")
        false
    }
  }
}

object SFTPManager {
  def apply(ftpInfo: SFtpServerInfo): SFTPManager =
    new SFTPManager(ftpInfo.ip, ftpInfo.port, ftpInfo.user, ftpInfo.password)
}

case class SFtpServerInfo(user: String,
                          password: String,
                          ip: String,
                          port: String)


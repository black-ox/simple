package com.ox.bigdata.util.sftp

import java.io._
import com.ox.bigdata.util.ftp.FtpManager
import com.ox.bigdata.util.log.LogSupport
import com.jcraft.jsch.ChannelSftp


class SFTPManager(server: String, port: String, user: String, password: String) extends
  FtpManager(server: String, port: String, user: String, password: String) with LogSupport {
  // private final val LOG: Logger = Logger.getLogger(classOf[SFTPChannel].getName)
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

  // test failed
  override def listFiles(parentPath: String): Array[String] = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSftp: ChannelSftp = getChannelSFTP(channel)

      val result = channelSftp.ls(parentPath).toArray
      val files = result.filterNot(x => x.toString.startsWith("d"))
      val names = files.toList.map(x => x.toString.split(" ").last)
      channelSftp.quit()
      channel.closeChannel()
      val array = new Array[String](names.size)
      names.copyToArray(array, 0)
      array
    }
    catch {
      case _: Exception =>
        //      LOG.debug("Upload file exception!")
        Nil.toArray
    }
  }

  override def listDirectories(parentPath: String): Array[String] = {
    try {
      val channel: SFTPChannel = getChannel()
      val ChannelSFTP: ChannelSftp = getChannelSFTP(channel)
      val result = ChannelSFTP.ls(parentPath).toArray
      ChannelSFTP.quit()
      channel.closeChannel()

      val directory = result.filter(x => x.toString.startsWith("d"))
      val folderstemp = directory.toList.map(x => x.toString.split(" ").last)
      val folders = folderstemp.filterNot(x => x == "." || x == "..")
      val array = new Array[String](folders.size)
      folders.copyToArray(array, 0)
      array
    }
    catch {
      case _: Throwable =>
        //       LOG.debug("getDirectory exception!")
        Nil.toArray
    }
  }

  //upload file ok
  override def upload(local: String, remote: String): Boolean = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSftp: ChannelSftp = getChannelSFTP(channel)
      MakeRemoteDirectory(channelSftp, remote)
      channelSftp.put(local, remote, ChannelSftp.OVERWRITE)
      channelSftp.quit()
      channel.closeChannel()
      true
    }
    catch {
      case _: Throwable =>
        //       LOG.debug("Upload file exception!")
        false
    }
  }

  override def upload(localStream: InputStream, remote: String): Unit = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSFTP: ChannelSftp = getChannelSFTP(channel)
      val remotepath = MakeRemoteDirectory(channelSFTP, remote)
      val filename = remote.substring(remote.lastIndexOf("/"))
      channelSFTP.put(localStream, remote, ChannelSftp.OVERWRITE)
      channelSFTP.quit()
      channelSFTP.quit()
      channel.closeChannel()
    }
    catch {
      case _: Throwable =>
      //        LOG.debug("Upload file exception!")
    }
  }

  //download file ok
  override def download(src: String, dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Unit = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSFTP: ChannelSftp = getChannelSFTP(channel)
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
      }
      channelSFTP.quit()
      channel.closeChannel()
    }
    catch {
      case _: Exception =>
      //        LOG.debug("Download file exception!")
    }
  }

  override def downloadFiles(src: List[String], dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Unit = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSFTP: ChannelSftp = getChannelSFTP(channel)
      src.map(e => {
        if (fileIsExists(channelSFTP, e)) {
          var index = e.lastIndexOf('/')
          if (index == -1) index = e.lastIndexOf('\\')
          val fileName = e.substring(index + 1, e.length)
          val localFile = new File(dst + "/" + fileName)
          if (!localFile.getParentFile.exists)
            localFile.getParentFile.mkdirs
          val is = new FileOutputStream(localFile)
          channelSFTP.get(e, is)
          is.close()
        }
      })
      channelSFTP.quit()
      channel.closeChannel()
    }
    catch {
      case _: Throwable =>
      //        LOG.debug("downloadFiles file exception!")
    }
  }

  override def deleteDirectory(remote: String): Int = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSFTP: ChannelSftp = getChannelSFTP(channel)
      var isDeleteDirectorySuccess: Int = 0
      val remoteWithoutPoint: String = remote.take(1) match {
        case "." => remote.drop(1)
        case _ => remote
      }
      isDeleteDirectorySuccess = deleteDirectory(channelSFTP, remoteWithoutPoint)
      channelSFTP.quit()
      channel.closeChannel()
      isDeleteDirectorySuccess
    }
    catch {
      case _: Throwable =>
        //      LOG.debug("delete Directory exception!")
        -1
    }
  }

  //delete directory test ok
  private def deleteDirectory(ChannelSFTP: ChannelSftp, remoteWithoutPoint: String): Int = {
    try {
      val result = ChannelSFTP.ls(remoteWithoutPoint).toArray
      val (dirlist, filelist) = result.partition(x => x.toString.startsWith("d"))
      val folderstemp = dirlist.toList.map(x => x.toString.split(" ").last)
      val folders = folderstemp.filterNot(x => x == "." || x == "..")
      val filenames = filelist.toList.map(x => x.toString.split(" ").last)
      if (!filenames.isEmpty)
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
      0
    }
    catch {
      case _: Throwable =>
        //       LOG.debug("delete Directory exception!")
        -1
    }
  }

  //delete foldor ok
  def deletefolder(folder: String): Unit = {
    try {
      val channel: SFTPChannel = getChannel();
      val channelSFTP: ChannelSftp = getChannelSFTP(channel);
      channelSFTP.rmdir(folder)
      channelSFTP.quit()
      channel.closeChannel()
    }
    catch {
      case _: Throwable =>
      //       LOG.debug("delete folder  exception!")
    }
  }

  //delete file ok
  override def delete(pathname: String): Unit = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSFTP: ChannelSftp = getChannelSFTP(channel)
      if (fileIsExists(channelSFTP, pathname)) {
        channelSFTP.rm(pathname)
      }
      channelSFTP.quit()
      channel.closeChannel()
    }
    catch {
      case _: Exception =>
      //       LOG.debug("delete file exception!")
    }
  }

  override def downloadByExt(srcDir: String, baseDstDir: String, ext: String): Unit = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSFTP: ChannelSftp = getChannelSFTP(channel)
      downloadByExt(channelSFTP, srcDir, baseDstDir, ext)
      channelSFTP.quit()
      channel.closeChannel()
    }
    catch {
      case _: Exception =>
      //        LOG.debug("downloadByExt file exception!")
    }
  }

  private def downloadByExt(ChannelSFTP: ChannelSftp, srcDir: String, baseDstDir: String, ext: String): Unit = {
    try {
      val result = ChannelSFTP.ls(srcDir).toArray
      val direction = result.partition(x => x.toString.startsWith("d"))
      val folderstemp = direction._1.toList.map(x => x.toString.split(" ").last)
      val folders = folderstemp.filterNot(x => x == "." || x == "..")
      val filenames = direction._2.toList.map(x => x.toString.split(" ").last)

      val file = new File(baseDstDir + "/test")
      if (!file.getParentFile.exists)
        file.getParentFile.mkdirs
      filenames.map(x => if (x.endsWith(ext))
        ChannelSFTP.get(s"$srcDir/$x", s"$baseDstDir/$x"))

      folders.foreach(x => downloadByExt(ChannelSFTP, s"$srcDir/$x", s"$baseDstDir/$x", ext))
    }
    catch {
      case _: Exception =>
      //        LOG.debug("downloadByExt file exception!")
    }
  }

  /**
    * 递归遍历和下载指定目录下面指定后缀名的文件
    * relativePath 相对路径，用来记录在ftp内当前目录的相对路径。在调用时该参数设置为""，后面递归调用时会设置为非空值。
    * srcDir 需要遍历的目录
    * baseDstDir 目标目录，必须是绝对路径
    * ext 文件的扩展名
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
      if (filenames.contains(filename))
        true
      else
        false
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


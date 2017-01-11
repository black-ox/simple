package com.ox.bigdata.util.ftp

import java.io._
import com.ox.bigdata.util.log.LogSupport
import com.ox.bigdata.util.sftp.SFTPManager
import org.apache.commons.net.PrintCommandListener
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPFile, FTPReply}

trait UploadSupport {
  def upload(serverinfo: FtpServerInfo, local: String, remote: String): Boolean
}

trait FTPUploadSupport extends UploadSupport {
  override def upload(serverinfo: FtpServerInfo, local: String, remote: String): Boolean = {
    FtpManager(serverinfo).upload(local, remote)
  }
}

class FtpManager(val server: String, val port: String, val user: String, val password: String) extends LogSupport {
  def getFileNames(parentPath: String, withParentPath: Boolean = false,
                   timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Array[String] = {
    var result = Array[String]()
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        ftp.setDataTimeout(timeout)
        val vparentPath = if (parentPath.endsWith("/")) parentPath else parentPath + "/"
        val files = ftp.listFiles(vparentPath)
        if (files != null && files.nonEmpty) {
          result = files.map(e =>
            if (withParentPath) vparentPath + e.getName else e.getName)
        }
        //close(ftp)
    }
    result
  }

  def upload(local: String, remote: String): Boolean = {
    var isUploadSuccess: Boolean = false
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        isUploadSuccess = upload(ftp, local, remote)
    }
    isUploadSuccess
  }

  def upload(localStream: InputStream, remote: String): Unit = {
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        upload(ftp, localStream, remote)
    }
  }

  def download(src: String, dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Unit = {
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        ftp.setFileType(FTP.BINARY_FILE_TYPE)
        ftp.enterLocalPassiveMode()
        ftp.setBufferSize(1024 * 1024)
        ftp.setDataTimeout(timeout)
        download(ftp, src, dst)
    }
  }

  def downloadFiles(src: List[String], dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Unit = {
    using {
      ftp =>
        connect(ftp)
        login(ftp)

        ftp.setFileType(FTP.BINARY_FILE_TYPE)
        ftp.enterLocalPassiveMode()
        ftp.setBufferSize(1024 * 1024)
        ftp.setDataTimeout(timeout)
        src.map(e => {
          var index = e.lastIndexOf('/')
          if (index == -1) index = e.lastIndexOf('\\')
          val fileName = e.substring(index + 1, e.length)
          val localFile = new File(dst + "/" + fileName)
          val is = new FileOutputStream(localFile)
          ftp.retrieveFile(e, is)
          //          if (!localFile.exists()) {
          //            log.error("Failed to download file " + e)
          //          }
          is.close()
        })
        close(ftp)
    }
  }

  def downloadByExt(srcDir: String, baseDstDir: String, ext: String): Unit = {
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        downloadByExt(ftp, "", srcDir, baseDstDir, ext)
    }
  }

  def delete(pathname: String): Unit = {
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        delete(ftp, pathname)
    }
  }

  private def delete(ftp: FTPClient, pathname: String): Boolean = {
    ftp.deleteFile(pathname)
    ftp.logout()
  }

  private def using(f: FTPClient => Unit): Unit = {
    val ftp = new FTPClient()
    try {
      f(ftp)
    } catch {
      case e: IOException =>
        e.printStackTrace()
        LOG.error(s"Could not connect to server due to ${e.getMessage}")
      case e: ConnectionException => LOG.error(e.getMessage)
      case e: LoginException => LOG.error(e.getMessage)
    } finally {
      close(ftp)
    }
  }

  private def close(ftp: FTPClient) {
    if (ftp.isConnected) {
      try {
        ftp.disconnect()
      }
      catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
  }

  private def MakeRemoteDirectory(ftp: FTPClient, remote: String): String = {
    def remotepathVerified(path: String): String = path.take(1) match {
      case "." => remotepathVerified(path.drop(1))
      case "/" => path.drop(1)
      case _ => path
    }

    val checkedRemotePath = remotepathVerified(remote)
    val directories = checkedRemotePath.split('/')

    directories.init.foldLeft(".")((dir, a) => {
      ftp.makeDirectory(dir + "/" + a)
      dir + "/" + a
    })

    checkedRemotePath
  }

  private def upload(ftp: FTPClient, local: String, remote: String): Boolean = {
    val input = new FileInputStream(local)
    upload(ftp, input, remote)
  }

  private def upload(ftp: FTPClient, localStream: InputStream, remote: String): Boolean = {
    ftp.setFileType(FTP.BINARY_FILE_TYPE)
    val remotePath = MakeRemoteDirectory(ftp, remote)
    val isUploadSuccessful = ftp.storeFile(s"./$remotePath", localStream)
    localStream.close()
    ftp.noop() // check that control connection is working OK
    ftp.logout()
    isUploadSuccessful
  }

  private def download(ftp: FTPClient, src: String, dst: String): Boolean = {
    ftp.setFileType(FTP.BINARY_FILE_TYPE)
    var index = src.lastIndexOf('/')
    if (index == -1) index = src.lastIndexOf('\\')
    val fileName = src.substring(index + 1, src.length)
    val localFile = new File(dst + "/" + fileName)
    val localStream = new FileOutputStream(localFile)
    val isDownloadSuccessful = ftp.retrieveFile(src, localStream)
    localStream.close()
    ftp.noop() // check that control connection is working OK
    ftp.logout()
    isDownloadSuccessful
  }

  private def login(ftp: FTPClient): Unit = {
    if (!ftp.login(user, password)) {
      ftp.logout()
      throw new LoginException("FTP server refused login.")
    }
  }

  private def connect(ftp: FTPClient): Unit = {
    if (port == "") {
      ftp.connect(server)
    } else {
      ftp.connect(server, port.toInt)
    }

    val reply = ftp.getReplyCode
    ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out))) //Open the debug info
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new ConnectionException("Ftp server refused connection")
    }
  }

  private def getFileName(fullPath: String): String = {
    var index = fullPath.lastIndexOf('/')
    if (index == -1) index = fullPath.lastIndexOf('\\')
    fullPath.substring(index + 1, fullPath.length)
  }

  /**
    *
    * relativePath
    * srcDir
    * baseDstDir
    * ext
    */
  private def downloadByExt(ftp: FTPClient, relativePath: String, srcDir: String, baseDstDir: String, ext: String): Unit = {
    val array = srcDir.split("/").toList
    array.foreach(x => if (x != "") ftp.changeWorkingDirectory(x))
    ftp.listFiles.foreach(f => {
      if (f.isFile) {
        if (f.getName.endsWith(ext)) {
          val dst = baseDstDir + "/" + relativePath
          val src = f.getName
          val file = new File(dst + "/" + f.getName)
          if (!file.exists) {
            if (!file.getParentFile.exists) file.getParentFile.mkdirs
            download(ftp, src, dst)
          }
          //ftp.deleteFile(src) // delete file on the FTP after download to local
        }
      }
      else if (f.isDirectory) {
        val relativeDir = if (relativePath == "") f.getName else relativePath + "/" + f.getName
        downloadByExt(ftp, relativeDir, f.getName, baseDstDir, ext)
      }
    })
    ftp.changeToParentDirectory()
  }

  def deleteDirectory(remote: String): Int = {
    var isDeleteDirectorySuccess: Int = 0
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        ftp.setFileType(FTP.BINARY_FILE_TYPE)
        val remoteWithoutPoint: String = remote.take(1) match {
          case "." => remote.drop(1)
          case _ => remote
        }
        //ftp.deleteFile(s"./$remoteWithoutPoint")//delete file (nonempty dir)
        //ftp.rmd(s"./$remoteWithoutPoint")//delete dir
        isDeleteDirectorySuccess = deleteDirectory(ftp, remoteWithoutPoint)
        ftp.noop() // check that control connection is working OK
        ftp.logout()
    }
    isDeleteDirectorySuccess
  }

  def listDirectories(remote: String): Array[String] = {
    var result: List[FTPFile] = Nil
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        val dirs = ftp.listDirectories(remote)
        ftp.noop() // check that control connection is working OK
        ftp.logout()
        result = dirs.toList
    }
    result.map(file => file.getName).toArray
  }

  def listFiles(remote: String): Array[String] = {
    var result: List[FTPFile] = Nil
    using {
      ftp =>
        connect(ftp)
        login(ftp)
        val files = ftp.listFiles(remote)
        ftp.noop()
        ftp.logout()
        result = files.toList
    }
    result.map(file => file.getName).toArray
  }


  private def deleteDirectory(ftp: FTPClient, remoteWithoutPoint: String): Int = {
    ftp.listFiles(s"./$remoteWithoutPoint").foreach(f => {
      if (f.isDirectory) {
        val fileName = f.getName
        val filePath = s"$remoteWithoutPoint/$fileName"
        deleteDirectory(ftp, filePath)
        ftp.rmd(s"./$filePath")
        ftp.changeWorkingDirectory(s"./$filePath/$fileName")
      }
      else if (f.isFile) {
        val fileName = f.getName
        ftp.deleteFile(s"./$remoteWithoutPoint/$fileName")
      }
    })
    val isDeleteDirectorySuccess = ftp.rmd(s"./$remoteWithoutPoint")
    isDeleteDirectorySuccess
  }
}

private class ConnectionException(message: String) extends Exception(message)

private class LoginException(message: String) extends Exception(message)

object FtpManager {
  val FTP_DATA_TIMEOUT_DEFAULT = 1000 * 30

  //time out 30 sec
  def apply(ftpInfo: FtpServerInfo): FtpManager = new SFTPManager(ftpInfo.ip, ftpInfo.port, ftpInfo.user, ftpInfo.password)

}

case class FtpServerInfo(user: String,
                         password: String,
                         ip: String,
                         port: String)

case class FtpConfigInfo(ftpServerInfo: FtpServerInfo,
                         filePathInfo: String)


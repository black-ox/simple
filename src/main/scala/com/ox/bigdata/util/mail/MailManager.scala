package com.ox.bigdata.util.mail

import java.util.Date
import javax.activation.{DataHandler, FileDataSource}
import javax.mail._
import javax.mail.internet._

import com.ox.bigdata.util.log.LogSupport

import scala.io.Source


class MailManager(mailServerInfo: MailServerInfo) extends LogSupport {
  this: GetTransport =>
  val mailSession = new MailServer(mailServerInfo).getMailSession

  def sendMail(eMailInfo: EMailInfo): Unit = {
    send(eMailInfo) {
      Unit =>
        val messageContent: Multipart = new MimeMultipart()
        messageContent.addBodyPart(getTextBodyPart(eMailInfo.content))
        messageContent
    }
  }

  def sendMail(eMailInfo: EMailInfo, filePath: String): Unit = {
    send(eMailInfo) {
      Unit =>
        val messageContent: Multipart = new MimeMultipart()
        messageContent.addBodyPart(getTextBodyPart(eMailInfo.content))
        messageContent.addBodyPart(getFileBodyPart(filePath))
        messageContent
    }
  }

  def sendMailByHtml(eMailInfo: EMailInfo, filePath: String): Unit = {
    send(eMailInfo) {
      Unit =>
        val messageContent: Multipart = new MimeMultipart()
        messageContent.addBodyPart(getTextBodyPart(eMailInfo.content))
        messageContent.addBodyPart(getHtmlBodyPartContent(filePath))
        messageContent
    }
  }

  private def getTextBodyPart(content: String): MimeBodyPart = {
    val bpTextContent = new MimeBodyPart()
    bpTextContent.setText(content)
    bpTextContent
  }

  private def getHtmlBodyPart(content: String): MimeBodyPart = {
    val bpTextContent = new MimeBodyPart()
    bpTextContent.setContent(content, "text/html; charset = utf-8")
    bpTextContent
  }

  private def getHtmlBodyPartContent(htmlPath: String): MimeBodyPart = {
    val bpTextContent = new MimeBodyPart()
    val htmlLines = Source.fromFile(htmlPath, "UTF-8").getLines().toList.mkString("\n")
    bpTextContent.setContent(htmlLines, "text/html; charset = utf-8")
    bpTextContent
  }

  private def getFileBodyPart(filePath: String): MimeBodyPart = {
    val bpFile = new MimeBodyPart()
    val fileSource = new FileDataSource(filePath)
    bpFile.setDataHandler(new DataHandler(fileSource))
    bpFile.setFileName(MimeUtility.encodeText(fileSource.getName(), "utf-8", null))
    bpFile
  }

  private def send(eMailInfo: EMailInfo)(f: Unit => Multipart): Unit = {
    val message: Message = new MimeMessage(mailSession)
    val from: InternetAddress = new InternetAddress(eMailInfo.fromAddress)
    val to: Array[Address] = eMailInfo.toAddress.map(address => new InternetAddress(address, false)).toArray
    message.setFrom(from)
    message.setRecipients(Message.RecipientType.TO, to)
    message.setSubject(eMailInfo.subject)
    message.setSentDate(new Date())
    message.setContent(f())

    transportMail(message)
  }

  private def transportMail(message: Message): Unit = {
    val transport = getTransport
    //    val transport = mailSession.getTransport("smtp")
    try {
      transport.connect()
      transport.sendMessage(message, message.getAllRecipients())
    } finally {
      transport.close()
    }
  }
}

object MailManager {
  def apply(mailServerInfo: MailServerInfo): MailManager = new MailManager(mailServerInfo) with GetTransport
}

//为了可测试性，增加此函数
trait GetTransport {
  this: MailManager =>
  def getTransport(): Transport = {
    mailSession.getTransport("smtp")
  }
}

case class EMailInfo(subject: String, fromAddress: String, toAddress: List[String], content: String)


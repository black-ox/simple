package com.ox.bigdata.util.mail

import java.util.Properties
import javax.mail.{Authenticator, PasswordAuthentication, Session}


case class MailServerInfo(mailServerHost: String, mailServerPort: String, isAuthenticated: String, isEnabledDebugMod: String, userName: String, passWord: String)

class MailServer(mailServerInfo: MailServerInfo) {

  private class MailAuthenticator(userName: String, passWord: String) extends Authenticator {
    override protected def getPasswordAuthentication: PasswordAuthentication = new PasswordAuthentication(userName, passWord)
  }

  private def getSystemProperties: Properties = {
    val sysProperties = new Properties()
    //    sysProperties.setProperty("mail.transport.protocol", "smtp")
    sysProperties.put("mail.smtp.host", mailServerInfo.mailServerHost)
    sysProperties.put("mail.smtp.port", mailServerInfo.mailServerPort)
    sysProperties.put("mail.smtp.auth", mailServerInfo.isAuthenticated)
    sysProperties.put("mail.debug", mailServerInfo.isEnabledDebugMod)
    sysProperties
  }

  private def getAuthenticator: Option[Authenticator] = {
    if (mailServerInfo.isAuthenticated == "true")
      Some(new MailAuthenticator(mailServerInfo.userName, mailServerInfo.passWord))
    else None
  }

  // mail session
  def getMailSession: Session = getAuthenticator match {
    case Some(authenticator) => Session.getInstance(getSystemProperties, authenticator)
    case None => Session.getDefaultInstance(getSystemProperties)
  }
}


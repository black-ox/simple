package com.ox.bigdata.util.sftp;

import com.jcraft.jsch.*;

import java.util.Map;
import java.util.Properties;

public class SFTPChannel {
    Session session = null;
    Channel channel = null;

    //  private static final Logger LOG = Logger.getLogger(SFTPChannel.class.getName());

    public ChannelSftp getChannel(Map<String, String> sftpDetails, int timeout) throws JSchException {

        String ftpHost = sftpDetails.get(SFTPConstants.SFTP_REQ_HOST);
        String port = sftpDetails.get(SFTPConstants.SFTP_REQ_PORT);
        String ftpUserName = sftpDetails.get(SFTPConstants.SFTP_REQ_USERNAME);
        String ftpPassword = sftpDetails.get(SFTPConstants.SFTP_REQ_PASSWORD);

        int ftpPort = SFTPConstants.SFTP_DEFAULT_PORT;
        if (port != null && !port.equals("")) {
            ftpPort = Integer.valueOf(port);
        }

        JSch jsch = new JSch();
        session = jsch.getSession(ftpUserName, ftpHost, ftpPort);
        //       LOG.debug("Session created.");
        if (ftpPassword != null) {
            session.setPassword(ftpPassword);
        }
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.setTimeout(timeout);
        session.connect();
        //       LOG.debug("Session connected.");

        //       LOG.debug("Opening Channel.");
        channel = session.openChannel("sftp");
        channel.connect();
        //       LOG.debug("Connected successfully to ftpHost = " + ftpHost + ",as ftpUserName = " + ftpUserName
        //               + ", returning: " + channel);
        return (ChannelSftp) channel;
    }

    public void closeChannel() throws Exception {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }
}


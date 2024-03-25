/*
* ChatPackagerJob.java
 */
package nz.co.spark.cg.packager.task;

import nz.co.spark.cg.shared.xml.XmlDomUtils;
import nz.co.spark.cg.shared.zip.ZipUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPHTTPClient;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Chat Packager Job.
 * @author rod
 * @since 2018-09-19
 */
public class ChatPackagerJob implements Job {
    private static final Logger log = LoggerFactory.getLogger(ChatPackagerJob.class);
    private static final String OUTPUT_DIRECTORY = "output.directory";
    private static final String OUTPUT_FTP_ENABLED = "output.ftp.enabled";
    private static final String OUTPUT_FTP_SERVER = "output.ftp.server";
    private static final String OUTPUT_FTP_PORT = "output.ftp.port";
    private static final String OUTPUT_FTP_USER = "output.ftp.user";
    private static final String OUTPUT_FTP_PASSWORD = "output.ftp.password";
    private static final String OUTPUT_FTP_PROXY = "output.ftp.proxy";
    private static final String OUTPUT_FTP_PROXY_PORT = "output.ftp.proxy.port";
    private static final String OUTPUT_FTP_PROXY_USER = "output.ftp.proxy.user";
    private static final String OUTPUT_FTP_PROXY_PASSWORD = "output.ftp.proxy.password";

    private static final String YES = "YES";
    private static final String XML = ".xml";

    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        String fileName = null;
        FTPClient ftpClient = null;
        InputStream inputStream = null;

        try {
            log.debug("Running Packager task... ");
            fileName = ZipUtils.generateZip(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_DIRECTORY), XML);
            XmlDomUtils.removeXML(new File(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_DIRECTORY)));
            if( ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_ENABLED).equalsIgnoreCase(YES) ) {
                log.debug("FTP enabled...");
                if( ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_PROXY).isEmpty() ) {
                    ftpClient = new FTPHTTPClient(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_PROXY),
                                                  Integer.valueOf(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_PROXY_PORT)),
                                                  ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_PROXY_USER),
                                                  ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_PROXY_PASSWORD));
                } else {
                    ftpClient = new FTPClient();
                }
                ftpClient.connect(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_SERVER),Integer.valueOf(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_PORT)));
                ftpClient.login(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_USER),ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_FTP_PASSWORD));
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                inputStream = new FileInputStream(ChatPackagerTimer.chatPackagerProperties.getProperty(OUTPUT_DIRECTORY)+File.separator+fileName);
                ftpClient.storeFile(fileName,inputStream);
                ZipUtils.removeZip(fileName);
                log.debug("FTP finished.");
            }
            log.debug("Packager task finished.");
        } catch ( Throwable e ) {
            e.printStackTrace();
            if( fileName != null ) {
                ZipUtils.removeZip(fileName);
            }
            log.error("Problems running task, the zip file has not been generated. Message:{}",e.getMessage());
        } finally {
            if( inputStream != null ) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                }
            }
            if( ftpClient != null ) {
                try {
                    ftpClient.disconnect();
                } catch (IOException e) {
                }
            }
        }
    }
}
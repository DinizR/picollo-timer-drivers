/*
* SocialCloserJob.java
 */
package nz.co.spark.cg.social.closer.task;

import nz.co.spark.cg.shared.model.Contact;
import nz.co.spark.cg.shared.model.ContactType;
import nz.co.spark.cg.shared.sql.ExtractionUtils;
import nz.co.spark.cg.shared.sql.SQLUtils;
import nz.co.spark.cg.shared.xml.XmlDomUtils;
import nz.co.spark.cg.shared.xml.XmlToContact;
import org.genesys.helper.GenesysUniversalContactService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Social Closer Job.
 * @author rod
 * @since 2018-10-05
 */
@DisallowConcurrentExecution
public class SocialCloserJob implements Job {
    private static final String DATABASE_LOCAL_DRIVER = "database.local.driver";
    private static final String DATABASE_LOCAL_URL = "database.local.url";
    private static final String DATABASE_LOCAL_USER = "database.local.user";
    private static final String DATABASE_LOCAL_PASSWORD = "database.local.password";

    private static final String DATABASE_DRIVER = "database.driver";
    private static final String DATABASE_URL = "database.url";
    private static final String DATABASE_USER = "database.user";
    private static final String DATABASE_PASSWORD = "database.password";

    private static final String OUTPUT_DIRECTORY = "output.directory";

    private static final String REGEX_PATTERN = "regex.pattern";
    private static final String REGEX_REPLACE = "regex.replace";
    private static final String FILTER_TYPE = "filter.type";

    private static final String CONTACT_SERVER_CLIENT_NAME = "contact.server.client-name";
    private static final String CONTACT_SERVER_HOST_PRIMARY = "contact.server.host.primary";
    private static final String CONTACT_SERVER_PORT_PRIMARY = "contact.server.port.primary";
    private static final String CONTACT_SERVER_HOST_SECONDARY = "contact.server.host.secondary";
    private static final String CONTACT_SERVER_PORT_SECONDARY = "contact.server.port.secondary";

    public SocialCloserJob() {
        SQLUtils.createLocalDB(
                SocialCloserTimer.configMap.get("database.local.driver"),
                SocialCloserTimer.configMap.get("database.local.url"),
                SocialCloserTimer.configMap.get("database.local.user"),
                SocialCloserTimer.configMap.get("database.local.password")
        );
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        Connection connection = null;
        Connection remoteConnection = null;
        GenesysUniversalContactService contactService = null;

        try {
            SocialCloserTimer.logger.info("Running Social Closer task... ");

            contactService = new GenesysUniversalContactService(
                    SocialCloserTimer.configMap.get(CONTACT_SERVER_HOST_PRIMARY),
                    Integer.valueOf(SocialCloserTimer.configMap.get(CONTACT_SERVER_PORT_PRIMARY)),
                    SocialCloserTimer.configMap.get(CONTACT_SERVER_CLIENT_NAME),
                    SocialCloserTimer.configMap.get(CONTACT_SERVER_HOST_SECONDARY),
                    Integer.valueOf(SocialCloserTimer.configMap.get(CONTACT_SERVER_PORT_SECONDARY))
            );
            try {
                contactService.openServiceWithWarmStandBy();
            } catch (Exception e) {
                SocialCloserTimer.logger.error("Failed to connect to Genesys UCS API.", e);
            }

            connection = SQLUtils.getDBConnection(
                    SocialCloserTimer.configMap.get(DATABASE_LOCAL_DRIVER),
                    SocialCloserTimer.configMap.get(DATABASE_LOCAL_URL),
                    SocialCloserTimer.configMap.get(DATABASE_LOCAL_USER),
                    SocialCloserTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
            );
            remoteConnection = SQLUtils.getDBConnection(
                    SocialCloserTimer.configMap.get(DATABASE_DRIVER),
                    SocialCloserTimer.configMap.get(DATABASE_URL),
                    SocialCloserTimer.configMap.get(DATABASE_USER),
                    SocialCloserTimer.configMap.get(DATABASE_PASSWORD)
            );

            updateNonFinished(connection,remoteConnection,contactService);
            SocialCloserTimer.logger.info("Social Closer task finished.");
        } catch ( Throwable e ) {
            SocialCloserTimer.logger.error("Problems running Social Closer task.",e);
            if( connection != null ) {
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                }
            }
        } finally {
            if( contactService != null ) {
                try {
                    contactService.releaseConnection();
                } catch (Exception e) {
                    SocialCloserTimer.logger.error("Problems closing UCS connection.",e);
                }
            }
            try {
                if( connection != null && ! connection.isClosed() ) {
                    connection.close();
                }
            } catch (SQLException e) {
                SocialCloserTimer.logger.error("Problems closing database connection.",e);
            }
            try {
                if( remoteConnection != null && ! remoteConnection.isClosed() ) {
                    remoteConnection.close();
                }
            } catch (SQLException e) {
                SocialCloserTimer.logger.error("Problems closing database connection.",e);
            }
        }
    }

    private void updateNonFinished(Connection connection,Connection remoteConnection,GenesysUniversalContactService contactService) throws SQLException, IOException, SAXException, ParserConfigurationException {
        final String LOCAL_QUERY = "SELECT id,startdate,enddate,xml,finished,timeshift FROM interaction " +
                                   "WHERE finished = false AND type='Social' "+
                                   "ORDER BY startdate;";
        final String REMOTE_QUERY= "SELECT ID,STARTDATE,ENDDATE,STRUCTUREDTEXT,TIMESHIFT FROM INTERACTION " +
                                   "WHERE ID = ? AND STATUS = '3' " +
                                   "ORDER BY STARTDATE";
        final String UPDT_XML_LOCAL = "UPDATE interaction " +
                                      "SET xml = ? " +
                                      "WHERE id = ?";
        final String UPDT_END_LOCAL = "UPDATE interaction "+
                                      "SET enddate = ?, " +
                                          "finished = true "+
                                      "WHERE id = ?";
        final PreparedStatement localQueryStatement;
        final PreparedStatement remoteQueryStatement;
        final PreparedStatement updateLocalEnd;
        final PreparedStatement updateLocalXML;
        final ResultSet localResultSet;
        final List<Contact> contacts = new ArrayList<>();
        ResultSet remoteResultSet;
        Clob clob;
        String xml;
        XmlToContact xmlToContact;
        Contact contact;
        LocalDateTime startDateTime;
        LocalDateTime endDateTime = null;

        localQueryStatement = connection.prepareStatement(LOCAL_QUERY);
        remoteQueryStatement = remoteConnection.prepareStatement(REMOTE_QUERY);
        updateLocalEnd = connection.prepareStatement(UPDT_END_LOCAL);
        updateLocalXML = connection.prepareStatement(UPDT_XML_LOCAL);
        localResultSet = localQueryStatement.executeQuery();

        while (localResultSet.next()) {
            remoteQueryStatement.setString(1,localResultSet.getString(1));
            remoteResultSet = remoteQueryStatement.executeQuery(); // Lookup records in remote db
            if( remoteResultSet.next() ) {
                startDateTime = remoteResultSet.getTimestamp(2).toLocalDateTime();

                if( remoteResultSet.getTimestamp(3) != null ) { // filled the enddate field
                    endDateTime = remoteResultSet.getTimestamp(3).toLocalDateTime();
                    updateLocalEnd.setTimestamp(1, Timestamp.valueOf(endDateTime));
                    updateLocalEnd.setString(2, remoteResultSet.getString(1));
                    updateLocalEnd.executeUpdate();
                }
                clob = remoteResultSet.getClob(4);
                if (clob == null) {
                    continue;
                }
                xml = clob.getSubString(1, (int) clob.length());
                xml = XmlDomUtils.transformXML(xml, SocialCloserTimer.configMap.get(REGEX_PATTERN), SocialCloserTimer.configMap.get(REGEX_REPLACE));
                if (XmlDomUtils.isXMLValid(xml)) {
                    updateLocalXML.setCharacterStream(1, new StringReader(xml), xml.length());
                    updateLocalXML.setString(2, localResultSet.getString(1));
                    updateLocalXML.executeUpdate();
                    // generate XML
                    if( remoteResultSet.getTimestamp(3) == null ) {
                        xmlToContact = new XmlToContact(remoteResultSet.getString(1), ContactType.Social, startDateTime.format(DateTimeFormatter.ISO_DATE_TIME),"", xml);
                    } else {
                        xmlToContact = new XmlToContact(remoteResultSet.getString(1),ContactType.Social, startDateTime.format(DateTimeFormatter.ISO_DATE_TIME), endDateTime.format(DateTimeFormatter.ISO_DATE_TIME), xml);
                    }
                    xmlToContact.transform();
                    contact = xmlToContact.getContact();
                    ExtractionUtils.addUCSAttributes(contact,SocialCloserTimer.configMap,contactService);
                    contacts.add(contact);
                } else {
                    SocialCloserTimer.logger.error("Invalid XML file: {}",xml);
                }
            }
        }
        ExtractionUtils.load(contacts, SocialCloserTimer.configMap.get(OUTPUT_DIRECTORY),connection, SocialCloserTimer.configMap.get(FILTER_TYPE));
    }
}
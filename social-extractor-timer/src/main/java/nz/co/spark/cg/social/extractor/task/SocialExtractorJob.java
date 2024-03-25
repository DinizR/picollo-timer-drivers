/*
* SocialExtractorJob.java
 */
package nz.co.spark.cg.social.extractor.task;

import nz.co.spark.cg.shared.model.Contact;
import nz.co.spark.cg.shared.model.ContactType;
import nz.co.spark.cg.shared.sql.ExtractionUtils;
import nz.co.spark.cg.shared.sql.SQLUtils;
import nz.co.spark.cg.shared.xml.XmlDomUtils;
import org.genesys.helper.GenesysUniversalContactService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.io.StringReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.List;

/**
 * Social Extractor Job.
 * @author rod
 * @since 2018-10-04
 */
@DisallowConcurrentExecution
public class SocialExtractorJob implements Job {
    private static final String DATABASE_DRIVER = "database.driver";
    private static final String DATABASE_URL = "database.url";
    private static final String DATABASE_USER = "database.user";
    private static final String DATABASE_PASSWORD = "database.password";
    private static final String OUTPUT_DIRECTORY = "output.directory";
    private static final String STARTING_TIMESTAMP = "execution.query.startpoint";
    private static final String FILTER_TYPE = "filter.type";
    private static final String TIMER_DELAY = "execution.timer.delay";
    private static final int MINUTE_IN_MILLI_SECONDS = 60000;

    private static final String DATABASE_LOCAL_DRIVER = "database.local.driver";
    private static final String DATABASE_LOCAL_URL    = "database.local.url";
    private static final String DATABASE_LOCAL_USER   = "database.local.user";
    private static final String DATABASE_LOCAL_PASSWORD = "database.local.password";

    private static final String CONTACT_SERVER_CLIENT_NAME = "contact.server.client-name";
    private static final String CONTACT_SERVER_HOST_PRIMARY = "contact.server.host.primary";
    private static final String CONTACT_SERVER_PORT_PRIMARY = "contact.server.port.primary";
    private static final String CONTACT_SERVER_HOST_SECONDARY = "contact.server.host.secondary";
    private static final String CONTACT_SERVER_PORT_SECONDARY = "contact.server.port.secondary";

    private static final String REGEX_PATTERN = "regex.pattern";
    private static final String REGEX_REPLACE = "regex.replace";

    public SocialExtractorJob() {
        SQLUtils.createLocalDB(SocialExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
            SocialExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
            SocialExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
            SocialExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
        );
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        Connection connection = null;
        GenesysUniversalContactService contactService = null;

        try {
            contactService = new GenesysUniversalContactService(
                    SocialExtractorTimer.configMap.get(CONTACT_SERVER_HOST_PRIMARY),
                    Integer.valueOf(SocialExtractorTimer.configMap.get(CONTACT_SERVER_PORT_PRIMARY)),
                    SocialExtractorTimer.configMap.get(CONTACT_SERVER_CLIENT_NAME),
                    SocialExtractorTimer.configMap.get(CONTACT_SERVER_HOST_SECONDARY),
                    Integer.valueOf(SocialExtractorTimer.configMap.get(CONTACT_SERVER_PORT_SECONDARY))
            );
            try {
                contactService.openServiceWithWarmStandBy();
            } catch (Exception e) {
                SocialExtractorTimer.logger.error("Failed to connect to Genesys UCS API.", e);
            }

            connection = SQLUtils.getDBConnection(
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
            );

            SocialExtractorTimer.logger.info("Running social extraction task...");
            extraction();
            List<Contact> contacts = ExtractionUtils.transformation(connection,
                    "SELECT id,startdate,enddate,xml,finished,timeshift FROM interaction " +
                          "WHERE type='Social' AND finished=false AND enddate IS NOT NULL "+
                          "ORDER BY startdate;",ContactType.Social,SocialExtractorTimer.configMap,contactService);
            ExtractionUtils.load(contacts,SocialExtractorTimer.configMap.get(OUTPUT_DIRECTORY),connection,SocialExtractorTimer.configMap.get(FILTER_TYPE));
            SocialExtractorTimer.logger.info("Social extraction task finished.");
        } catch ( Throwable e ) {
            SocialExtractorTimer.logger.error("Problems running social extraction task.",e);
        } finally {
            if( contactService != null ) {
                try {
                    contactService.releaseConnection();
                } catch (Exception e) {
                    SocialExtractorTimer.logger.error("Problems closing UCS connection.",e);
                }
            }
            if( connection != null ) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    SocialExtractorTimer.logger.error("Problems closing database connection.",e);
                }
            }
        }
    }

    private void extraction() {
        final String START_QUERY = "SELECT ID,STARTDATE,ENDDATE,STRUCTUREDTEXT,TIMESHIFT FROM INTERACTION " +
                                   "WHERE MEDIATYPEID = 'facebooksession' AND STARTDATE BETWEEN ? AND ? " +
                                   "ORDER BY STARTDATE";
        final String DATE_QUERY =  "SELECT STARTDATE FROM INTERACTION WHERE MEDIATYPEID = 'facebooksession' AND ID = ?";
        final String NORMAL_QUERY = "SELECT ID,STARTDATE,ENDDATE,STRUCTUREDTEXT,TIMESHIFT FROM INTERACTION " +
                                    "WHERE MEDIATYPEID = 'facebooksession' AND STARTDATE > ? " +
                                    "ORDER BY STARTDATE";
        final String LASTID_QUERY = "SELECT ID FROM interaction WHERE type='Social' ORDER BY sequence DESC LIMIT 1;";
        final String INSERT_LOCAL = "INSERT INTO interaction (id,type,startdate,enddate,xml,timeshift) VALUES (?,?,?,?,?,?);";
        final String LOCAL_QUERY = "SELECT id FROM interaction WHERE id = ? AND type = 'Social';";
        final String UPDATE_QUERY = "UPDATE INTERACTION " +
                                    "SET STRUCTUREDTEXT = ? "+
                                    "WHERE ID = ?";
        Connection remoteConnection = null;
        Connection localConnection = null;
        ResultSet remoteResultSet;
        ResultSet localResultSet;

        if( SocialExtractorTimer.configMap.isEmpty() ) {
            SocialExtractorTimer.logger.error("Configuration file error running Social Extractor timer function. You should create a \"social-extractor.properties file\".");
            return;
        }
        try {
            remoteConnection = SQLUtils.getDBConnection(
                SocialExtractorTimer.configMap.get(DATABASE_DRIVER),
                SocialExtractorTimer.configMap.get(DATABASE_URL),
                SocialExtractorTimer.configMap.get(DATABASE_USER),
                SocialExtractorTimer.configMap.get(DATABASE_PASSWORD)
            );
            localConnection = SQLUtils.getDBConnection(
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
                SocialExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
            );

            PreparedStatement statementStart = remoteConnection.prepareStatement(START_QUERY);
            PreparedStatement statementDate = remoteConnection.prepareStatement(DATE_QUERY);
            PreparedStatement statementNormal = remoteConnection.prepareStatement(NORMAL_QUERY);
            PreparedStatement statementLastID = localConnection.prepareStatement(LASTID_QUERY);
            PreparedStatement statementInsert = localConnection.prepareStatement(INSERT_LOCAL);
            PreparedStatement statementVerify = localConnection.prepareStatement(LOCAL_QUERY);
            PreparedStatement statementUpdate = remoteConnection.prepareStatement(UPDATE_QUERY);

            if( ! SQLUtils.countIsGreaterThanZero(localConnection,"SELECT COUNT(*) FROM interaction WHERE type='Social';") ) {
                SocialExtractorTimer.logger.debug("Local DB is empty.");
                ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
                ZonedDateTime beginTime;

                if( SocialExtractorTimer.configMap.get(STARTING_TIMESTAMP).isEmpty() ) {
                    long minus = 5;
                    if( SocialExtractorTimer.configMap.get(TIMER_DELAY) != null && ! SocialExtractorTimer.configMap.get(TIMER_DELAY).isEmpty() ) {
                        minus = Long.valueOf(SocialExtractorTimer.configMap.get(TIMER_DELAY));
                    }
                    beginTime = now.minus(minus*MINUTE_IN_MILLI_SECONDS, ChronoField.MILLI_OF_DAY.getBaseUnit());
                } else {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    beginTime = LocalDateTime.parse(SocialExtractorTimer.configMap.get(STARTING_TIMESTAMP),formatter).atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC);
                }
                statementStart.setTimestamp(1,Timestamp.valueOf(beginTime.toLocalDateTime()));
                statementStart.setTimestamp(2,Timestamp.valueOf(now.toLocalDateTime()));
                SocialExtractorTimer.logger.debug("Querying first query with from start:{} to end:{}",beginTime,now);
                remoteResultSet = statementStart.executeQuery();
            } else {
                SocialExtractorTimer.logger.debug("Local DB is not empty.");
                String lastID = null;
                ResultSet dateResultset;

                localResultSet = statementLastID.executeQuery();
                if( localResultSet.next() ) {
                    lastID = localResultSet.getString(1);
                }
                statementDate.setString(1,lastID);
                SocialExtractorTimer.logger.debug("lastID={}",lastID);
                dateResultset = statementDate.executeQuery();
                if( dateResultset.next() ) {
                    SocialExtractorTimer.logger.debug("Date from local db: {}",dateResultset.getTimestamp(1));
                    statementNormal.setTimestamp(1,dateResultset.getTimestamp(1));
                } else {
                    SocialExtractorTimer.logger.debug("Nothing found in remote database.");
                    return;
                }
                remoteResultSet = statementNormal.executeQuery();
            }

            ResultSet rs;
            Clob clob;
            String xml;
            while( remoteResultSet.next() ) {
                // clear
                clob = remoteResultSet.getClob(4);
                if( clob == null ) {
                    continue;
                }
                xml = clob.getSubString(1, (int) clob.length());
                xml = XmlDomUtils.transformXML(xml, SocialExtractorTimer.configMap.get(REGEX_PATTERN), SocialExtractorTimer.configMap.get(REGEX_REPLACE));
                if( XmlDomUtils.isXMLValid(xml) ) {
                    statementUpdate.setCharacterStream(1, new StringReader(xml), xml.length());
                    statementUpdate.setString(2, remoteResultSet.getString(1));
                    statementUpdate.executeUpdate();
                }
                statementVerify.setString(1, remoteResultSet.getString(1));
                rs = statementVerify.executeQuery();
                if (!rs.next()) {
                    statementInsert.setString(1, remoteResultSet.getString(1));
                    statementInsert.setString(2, ContactType.Social.toString());
                    statementInsert.setTimestamp(3, remoteResultSet.getTimestamp(2));

                    if( remoteResultSet.getTimestamp(3) != null ) {
                          statementInsert.setTimestamp(4, remoteResultSet.getTimestamp(3));
                    } else {
                        statementInsert.setTimestamp(4, null);
                    }
                    statementInsert.setCharacterStream(5, new StringReader(xml), xml.length());
                    statementInsert.setInt(6,remoteResultSet.getInt(5));
                    statementInsert.executeUpdate();
                    localConnection.commit();
                }
            }
            remoteConnection.commit();
        } catch (SQLException e) {
            SocialExtractorTimer.logger.error("Error running or connecting to the database.",e);
        } catch (ClassNotFoundException e) {
            SocialExtractorTimer.logger.error("Error loading database driver.",e);
        } finally {
            try {
                if( remoteConnection != null ) {
                    remoteConnection.close();
                }
                if( localConnection != null ) {
                    localConnection.close();
                }
            } catch (SQLException e) {
            }
        }
    }
}
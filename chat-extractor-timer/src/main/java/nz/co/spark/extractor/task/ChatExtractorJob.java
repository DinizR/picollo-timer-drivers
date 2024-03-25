/*
* ChatExtractorJob.java
 */
package nz.co.spark.extractor.task;

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
 * Chat Extractor Job.
 * @author rod
 * @since 2018-09-13
 */
@DisallowConcurrentExecution
public class ChatExtractorJob implements Job {
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

    public ChatExtractorJob() {
        SQLUtils.createLocalDB(
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
        );
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        Connection connection = null;
        GenesysUniversalContactService contactService = null;

        try {
            contactService = new GenesysUniversalContactService(
                    ChatExtractorTimer.configMap.get(CONTACT_SERVER_HOST_PRIMARY),
                    Integer.valueOf(ChatExtractorTimer.configMap.get(CONTACT_SERVER_PORT_PRIMARY)),
                    ChatExtractorTimer.configMap.get(CONTACT_SERVER_CLIENT_NAME),
                    ChatExtractorTimer.configMap.get(CONTACT_SERVER_HOST_SECONDARY),
                    Integer.valueOf(ChatExtractorTimer.configMap.get(CONTACT_SERVER_PORT_SECONDARY))
            );
            try {
                contactService.openServiceWithWarmStandBy();
            } catch (Exception e) {
                ChatExtractorTimer.logger.error("Failed to connect to Genesys UCS API.", e);
            }
            connection = SQLUtils.getDBConnection(
                    ChatExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
                    ChatExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
                    ChatExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
                    ChatExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
            );

            ChatExtractorTimer.logger.info("Running chat extraction task...");
            extraction();
            List<Contact> contacts = ExtractionUtils.transformation(connection,
                            "SELECT id,startdate,enddate,xml,finished,timeshift FROM interaction " +
                                   "WHERE type='Chat' AND finished = false AND enddate IS NOT NULL "+
                                   "ORDER BY startdate;",ContactType.Chat,ChatExtractorTimer.configMap,contactService);

            ExtractionUtils.load(contacts,ChatExtractorTimer.configMap.get(OUTPUT_DIRECTORY),connection,ChatExtractorTimer.configMap.get(FILTER_TYPE));
            ChatExtractorTimer.logger.info("Chat extraction task finished.");
        } catch ( Throwable e ) {
            ChatExtractorTimer.logger.error("Problems running chat extraction task.",e);
        } finally {
            if( contactService != null ) {
                try {
                    contactService.releaseConnection();
                } catch (Exception e) {
                    ChatExtractorTimer.logger.error("Problems closing UCS connection.",e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    ChatExtractorTimer.logger.error("Problems closing database connection.",e);
                }
            }
        }
    }

    private void extraction() {
        final String START_QUERY = "SELECT ID,STARTDATE,ENDDATE,STRUCTUREDTEXT,TIMESHIFT FROM INTERACTION " +
                                   "WHERE MEDIATYPEID = 'chat' AND STARTDATE BETWEEN ? AND ? " +
                                   "ORDER BY STARTDATE";
        final String DATE_QUERY =  "SELECT STARTDATE FROM INTERACTION WHERE MEDIATYPEID = 'chat' AND ID = ?";
        final String NORMAL_QUERY = "SELECT ID,STARTDATE,ENDDATE,STRUCTUREDTEXT,TIMESHIFT FROM INTERACTION " +
                                    "WHERE (MEDIATYPEID = 'chat' AND STARTDATE > ?) " +
                                    "ORDER BY STARTDATE";
        final String LASTID_QUERY = "SELECT id FROM interaction WHERE type='Chat' ORDER BY sequence DESC LIMIT 1;";
        final String INSERT_LOCAL = "INSERT INTO interaction (id,type,startdate,enddate,xml,timeshift) VALUES (?,?,?,?,?,?);";
        final String LOCAL_QUERY = "SELECT id FROM interaction WHERE id = ? AND type = 'Chat'";
        final String UPDATE_QUERY = "UPDATE INTERACTION " +
                                    "SET STRUCTUREDTEXT = ? "+
                                    "WHERE ID = ?";
        Connection remoteConnection = null;
        Connection localConnection = null;
        ResultSet remoteResultSet;
        ResultSet localResultSet;

        if( ChatExtractorTimer.configMap.isEmpty() ) {
            ChatExtractorTimer.logger.error("Configuration file error running Chat Extractor timer function. You should create a \"chat-extractor.properties file\".");
            return;
        }
        try {
            remoteConnection = SQLUtils.getDBConnection(
                ChatExtractorTimer.configMap.get(DATABASE_DRIVER),
                ChatExtractorTimer.configMap.get(DATABASE_URL),
                ChatExtractorTimer.configMap.get(DATABASE_USER),
                ChatExtractorTimer.configMap.get(DATABASE_PASSWORD)
            );
            localConnection = SQLUtils.getDBConnection(
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_DRIVER),
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_URL),
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_USER),
                ChatExtractorTimer.configMap.get(DATABASE_LOCAL_PASSWORD)
            );

            PreparedStatement statementStart = remoteConnection.prepareStatement(START_QUERY);
            PreparedStatement statementDate = remoteConnection.prepareStatement(DATE_QUERY);
            PreparedStatement statementNormal = remoteConnection.prepareStatement(NORMAL_QUERY);
            PreparedStatement statementLastID = localConnection.prepareStatement(LASTID_QUERY);
            PreparedStatement statementInsert = localConnection.prepareStatement(INSERT_LOCAL);
            PreparedStatement statementVerify = localConnection.prepareStatement(LOCAL_QUERY);
            PreparedStatement statementUpdate = remoteConnection.prepareStatement(UPDATE_QUERY);

            if( ! SQLUtils.countIsGreaterThanZero(localConnection,"SELECT COUNT(*) FROM interaction WHERE type = 'Chat';") ) {
                ChatExtractorTimer.logger.debug("Local DB is empty.");
                ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
                ZonedDateTime beginTime;

                if( ChatExtractorTimer.configMap.get(STARTING_TIMESTAMP).isEmpty() ) {
                    long minus = 5;
                    if( ChatExtractorTimer.configMap.get(TIMER_DELAY) != null && ! ChatExtractorTimer.configMap.get(TIMER_DELAY).isEmpty() ) {
                        minus = Long.valueOf(ChatExtractorTimer.configMap.get(TIMER_DELAY));
                    }
                    beginTime = now.minus(minus*MINUTE_IN_MILLI_SECONDS, ChronoField.MILLI_OF_DAY.getBaseUnit());
                } else {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    beginTime = LocalDateTime.parse(ChatExtractorTimer.configMap.get(STARTING_TIMESTAMP),formatter).atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC);
                }
                statementStart.setTimestamp(1,Timestamp.valueOf(beginTime.toLocalDateTime()));
                statementStart.setTimestamp(2,Timestamp.valueOf(now.toLocalDateTime()));
                ChatExtractorTimer.logger.debug("Querying first query with from start:{} to end:{}",beginTime,now);
                remoteResultSet = statementStart.executeQuery();
            } else {
                ChatExtractorTimer.logger.debug("Local DB is not empty.");
                String lastID = null;
                ResultSet dateResultset;

                localResultSet = statementLastID.executeQuery();
                if( localResultSet.next() ) {
                    lastID = localResultSet.getString(1);
                }
                statementDate.setString(1,lastID);
                ChatExtractorTimer.logger.debug("lastID={}",lastID);
                dateResultset = statementDate.executeQuery();
                if( dateResultset.next() ) {
                    ChatExtractorTimer.logger.debug("Date from local db: {}",dateResultset.getTimestamp(1));
                    statementNormal.setTimestamp(1,dateResultset.getTimestamp(1));
                } else {
                    ChatExtractorTimer.logger.debug("Nothing found in remote database.");
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
                xml = XmlDomUtils.transformXML(xml,ChatExtractorTimer.configMap.get(REGEX_PATTERN),ChatExtractorTimer.configMap.get(REGEX_REPLACE));
                if( XmlDomUtils.isXMLValid(xml) ) {
                    statementUpdate.setCharacterStream(1, new StringReader(xml), xml.length());
                    statementUpdate.setString(2, remoteResultSet.getString(1));
                    statementUpdate.executeUpdate();
                }
                statementVerify.setString(1, remoteResultSet.getString(1));
                rs = statementVerify.executeQuery();
                if (!rs.next()) {
                    statementInsert.setString(1, remoteResultSet.getString(1));
                    statementInsert.setString(2, ContactType.Chat.toString());
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
            ChatExtractorTimer.logger.error("Error running or connecting to the database.",e);
        } catch (ClassNotFoundException e) {
            ChatExtractorTimer.logger.error("Error loading database driver. message={}",e.getMessage(),e);
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
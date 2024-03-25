package nz.co.spark.cg.social.cleaner.task;

import nz.co.spark.cg.shared.model.ContactType;
import nz.co.spark.cg.shared.sql.SQLUtils;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.model.TriggerType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.sql.*;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class SocialCleanerJobTest {
    private Connection memConnection = null;
    private SocialCleanerTimer socialCleanerTimer;

    @Before
    public void setup() throws ConfigurationException, SQLException, ClassNotFoundException {
        socialCleanerTimer = new SocialCleanerTimer();
        socialCleanerTimer.loadConfig();
        memConnection = createLocalDB();
    }

    @Test
    public void checkTimerConfig() {
        assertEquals(socialCleanerTimer.getTriggerType(), TriggerType.CRON_EXPRESSION_TRIGGER);
        assertEquals(socialCleanerTimer.cronExpression(),"0 0/10 * ? * * *");
        assertEquals(socialCleanerTimer.getJobClass(),SocialCleanerJob.class);
        assertEquals(socialCleanerTimer.isConfigEnabled(),true);
    }

    @Test
    public void checkCleanerJob() throws SQLException {
        SocialCleanerJob socialCleanerJob = new SocialCleanerJob();
        String sql = "SELECT COUNT(*) FROM interaction WHERE type='Social';";
        Statement statement = memConnection.createStatement();
        int record = -1;

        createMockData();
        memConnection.commit();
        socialCleanerJob.execute(null);
        memConnection.commit();
        ResultSet resultSet = statement.executeQuery(sql);
        if( resultSet.next() ) {
            record = resultSet.getInt(1);
        }

        assertEquals(record,0);
    }

    private Connection createLocalDB() throws SQLException, ClassNotFoundException {
        Connection connection;
        Statement statement;

        connection = SQLUtils.getDBConnection(
                SocialCleanerTimer.configMap.get("database.driver"),
                SocialCleanerTimer.configMap.get("database.url"),
                SocialCleanerTimer.configMap.get("database.user"),
                SocialCleanerTimer.configMap.get("database.password"));
        connection.setAutoCommit(true);
        statement = connection.createStatement();
        statement.execute("CREATE SEQUENCE IF NOT EXISTS INTERACTION_SEQ;");
        statement.execute("CREATE TABLE IF NOT EXISTS interaction(id CHAR(18) PRIMARY KEY,sequence BIGINT DEFAULT INTERACTION_SEQ.NEXTVAL,type CHAR(20),startdate TIMESTAMP,enddate TIMESTAMP,xml CLOB, finished BOOLEAN DEFAULT false);");
        statement.execute("CREATE INDEX IF NOT EXISTS date_index_interaction ON interaction(startdate);");
        statement.execute("CREATE INDEX IF NOT EXISTS seq_index_interaction ON interaction(sequence);");
        return connection;
    }

    private void createMockData() throws SQLException {
        final String INSERT_LOCAL = "INSERT INTO interaction (id,type,startdate,enddate,xml,finished) VALUES (?,?,?,?,?,?);";
        final PreparedStatement preparedStatement = memConnection.prepareStatement(INSERT_LOCAL);
        final LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
        String xml = "<?xml version=\"1.0\"?>" +
                "<chatTranscript startAt=\"2018-09-25T02:43:06Z\" sessionId=\"0002YaDSM5P0P9DK\">" +
                "<newParty userId=\"04395BA9A0BAD5BB\" timeShift=\"1\" visibility=\"ALL\" eventId=\"1\">" +
                "<userInfo personId=\"\" userNick=\"Nick Woods\" userType=\"CLIENT\" protocolType=\"FLEX\" timeZoneOffset=\"0\"/>" +
                "<userData>" +
                "<item key=\"BroadbandDiagnosticJourney\"/>" +
                "<item key=\"BroadbandDiagnosticServiceID\"/>" +
                "<item key=\"ConnectionID\">041002bcbb91e423</item>" +
                "<item key=\"EmailAddress\">njwoods01@gmail.com</item>" +
                "<item key=\"FirstName\">Nick</item>" +
                "<item key=\"GCTI_LanguageCode\">en-NZ</item>" +
                "<item key=\"IPaddress\">203.96.73.12</item>" +
                "<item key=\"IdentifyCreateContact\">3</item>" +
                "<item key=\"LastName\">Woods</item>" +
                "<item key=\"MediaType\">chat</item>" +
                "<item key=\"PageTag\">HelpFibre</item>" +
                "<item key=\"TimeZone\">720</item>" +
                "<item key=\"UserAgent\">Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; Zoom 3.6.0; rv:11.0) like Gecko</item>" +
                "<item key=\"VHQueue\"/>" +
                "<item key=\"_genesys_pageTitle\">Order tracker Page | Spark NZ</item>" +
                "<item key=\"_genesys_referrer\"/>" +
                "<item key=\"_genesys_source\">web</item>" +
                "<item key=\"_genesys_url\">https://www.spark.co.nz/secure/myspark/ordertracker/?orderId=1-173235038645&amp;utm_medium=email&amp;utm_source=consumer</item>" +
                "</userData>" +
                "</newParty>" +
                "<newParty userId=\"04395B6B2EDB46FB\" timeShift=\"8\" visibility=\"ALL\" eventId=\"2\">" +
                "<userInfo personId=\"\" userNick=\"Spark\" userType=\"EXTERNAL\" protocolType=\"ESP\" timeZoneOffset=\"0\"/>" +
                "</newParty>" +
                "<message userId=\"04395B6B2EDB46FB\" timeShift=\"8\" visibility=\"ALL\" eventId=\"3\">" +
                "<msgText>My email address: njwoods01@gmail.com</msgText>" +
                "</message>" +
                "<message userId=\"04395B6B2EDB46FB\" timeShift=\"8\" visibility=\"ALL\" eventId=\"4\">" +
                "<msgText>My question: Hi thereI&apos;m currently going through the process of getting fibre through spark. I have received the modem and fibre is already installed at my house. What are the next steps? I see there is a connection date of 03/10, is it possible the connection date could come forward? Also, does this connection date involve someone coming to the property to connect the internet and would someone have to be home for it? Couldn&apos;t find the answers online.Thanks,Nick</msgText>" +
                "</message>" +
                "<message userId=\"04395B6B2EDB46FB\" timeShift=\"8\" visibility=\"ALL\" eventId=\"5\">" +
                "<msgText>We are pretty busy right now, we will do our best to help you as soon as possible.</msgText>" +
                "</message>" +
                "<newParty userId=\"04395BA9A1C0D605\" timeShift=\"262\" visibility=\"ALL\" eventId=\"6\">" +
                "<userInfo personId=\"4940\" userNick=\"Loretta\" userType=\"AGENT\" protocolType=\"BASIC\" timeZoneOffset=\"720\"/>" +
                "</newParty>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"347\" visibility=\"ALL\" eventId=\"8\">" +
                "<msgText treatAs=\"NORMAL\">Hello Nick</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"412\" visibility=\"ALL\" eventId=\"10\">" +
                "<msgText treatAs=\"NORMAL\">Nick please can you provide me with your Fibre order number so i can investigate further Thanks</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A0BAD5BB\" timeShift=\"681\" visibility=\"ALL\" eventId=\"15\">" +
                "<msgText msgType=\"text\">Hi Loretta. Order number: 1-173235038645</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"715\" visibility=\"ALL\" eventId=\"17\">" +
                "<msgText treatAs=\"NORMAL\">Thank you</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"747\" visibility=\"ALL\" eventId=\"19\">" +
                "<msgText treatAs=\"NORMAL\">I am checking the order now. Could I just get you to confirm your full name and address for verification please? \uD83D\uDE0A</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A0BAD5BB\" timeShift=\"790\" visibility=\"ALL\" eventId=\"23\">" +
                "<msgText msgType=\"text\">Nicholas Woods</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A0BAD5BB\" timeShift=\"798\" visibility=\"ALL\" eventId=\"25\">" +
                "<msgText msgType=\"text\">187 Lorn Stree Invercargill</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"816\" visibility=\"ALL\" eventId=\"27\">" +
                "<msgText treatAs=\"NORMAL\">Thanks Nic</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"821\" visibility=\"ALL\" eventId=\"29\">" +
                "<msgText treatAs=\"NORMAL\">Thanks Nick</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"898\" visibility=\"ALL\" eventId=\"31\">" +
                "<msgText treatAs=\"NORMAL\">We can only remotely activate your Fibre connection at the above address on 03/10/2018 because there is still an active connection at that address with another provider which will only be disconnected on 02/10/2018.</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"982\" visibility=\"ALL\" eventId=\"35\">" +
                "<msgText treatAs=\"NORMAL\">If you know whom the previous occupants are then ask them if they can disconnect their services earlier.  They will need to contact their provider and their provider will need to contact chorus to change the disconnection date.</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A0BAD5BB\" timeShift=\"1174\" visibility=\"ALL\" eventId=\"65\">" +
                "<msgText msgType=\"text\">OK that makes sense. I will see if we can get in touch with the previous occupants. If we are able to get them to disconnect it earlier do we have to let you guys know to change the connection date or does that happen automatically?</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"1240\" visibility=\"ALL\" eventId=\"69\">" +
                "<msgText treatAs=\"NORMAL\">Please contact us again so we can check to see and we will need to reschedule for you.</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A0BAD5BB\" timeShift=\"1314\" visibility=\"ALL\" eventId=\"78\">" +
                "<msgText msgType=\"text\">Great, thank you. So I assume that everything is done remotely and will not require a visit from anyone?</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"1341\" visibility=\"ALL\" eventId=\"80\">" +
                "<msgText treatAs=\"NORMAL\">Yes all activated remotely</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A0BAD5BB\" timeShift=\"1366\" visibility=\"ALL\" eventId=\"87\">" +
                "<msgText msgType=\"text\">Awesome, thanks for your help \uD83D\uDE03</msgText>" +
                "</message>" +
                "<message userId=\"04395BA9A1C0D605\" timeShift=\"1399\" visibility=\"ALL\" eventId=\"89\">" +
                "<msgText treatAs=\"NORMAL\">You&apos;re welcome. Thanks for using our chat service and have a great day \uD83D\uDE03\uD83D\uDC4D</msgText>" +
                "</message>" +
                "<partyLeft userId=\"04395BA9A0BAD5BB\" timeShift=\"1437\" visibility=\"ALL\" eventId=\"90\" askerId=\"04395BA9A0BAD5BB\">" +
                "<reason>left</reason>" +
                "</partyLeft>" +
                "<partyLeft userId=\"04395BA9A1C0D605\" timeShift=\"1437\" visibility=\"ALL\" eventId=\"91\" askerId=\"04395BA9A1C0D605\">" +
                "<reason code=\"2\">left with request to close forcedly</reason>" +
                "</partyLeft>" +
                "</chatTranscript>";

        preparedStatement.setString(1,"00027aDEQEY00TYA");
        preparedStatement.setString(2, ContactType.Chat.toString());
        preparedStatement.setTimestamp(3, Timestamp.valueOf(yesterday));
        preparedStatement.setNull(4, Types.TIMESTAMP);
        preparedStatement.setCharacterStream(5, new StringReader(xml), xml.length());
        preparedStatement.setBoolean(6,false);
        preparedStatement.executeUpdate();

        preparedStatement.setString(1,"00027aDEQEY00TYB");
        preparedStatement.setString(2, ContactType.Social.toString());
        preparedStatement.setTimestamp(3, Timestamp.valueOf(yesterday));
        preparedStatement.setNull(4, Types.TIMESTAMP);
        preparedStatement.setCharacterStream(5, new StringReader(xml), xml.length());
        preparedStatement.setBoolean(6,false);
        preparedStatement.executeUpdate();

        memConnection.commit();
    }

    @After
    public void tearDown() throws SQLException {
        memConnection.close();
    }
}

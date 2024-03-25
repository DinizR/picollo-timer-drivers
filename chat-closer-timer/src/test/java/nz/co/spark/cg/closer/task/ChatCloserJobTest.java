package nz.co.spark.cg.closer.task;

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
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

public class ChatCloserJobTest {
    private static final String DATABASE_DRIVER = "database.driver";
    private static final String DATABASE_URL = "database.url";
    private static final String DATABASE_USER = "database.user";
    private static final String DATABASE_PASSWORD = "database.password";
    private ChatCloserTimer chatDBCleanerTimer;
    private Connection localMemConnection = null;
    private Connection remoteMemConnection = null;
    private LocalDateTime now = LocalDateTime.now();

    @Before
    public void setup() throws ConfigurationException, SQLException, ClassNotFoundException {
        chatDBCleanerTimer = new ChatCloserTimer();
        chatDBCleanerTimer.loadConfig();
        createPseudoOracleDB();
        localMemConnection = createLocalDB();
        createMockData();
    }

    @Test
    public void checkTimerConfig() {
        assertEquals(chatDBCleanerTimer.getTriggerType(), TriggerType.CRON_EXPRESSION_TRIGGER);
        assertEquals(chatDBCleanerTimer.cronExpression(),"0 0/1 * ? * * *");
        assertEquals(chatDBCleanerTimer.getJobClass(),ChatCloserJob.class);
        assertEquals(chatDBCleanerTimer.isConfigEnabled(),true);
    }

    @Test
    public void checkChatCloserJob() throws SQLException {
        ChatCloserJob chatCloserJob = new ChatCloserJob();
        String sql = "SELECT id,sequence,startdate,enddate,xml,finished FROM interaction WHERE type='Chat';";
        Statement statement = localMemConnection.createStatement();
        Timestamp endDate = null;
        boolean finished = false;

        chatCloserJob.execute(null);
        ResultSet resultSet = statement.executeQuery(sql);
        if( resultSet.next() ) {
            endDate = resultSet.getTimestamp(4);
            finished = resultSet.getBoolean(6);
        }
        //assertEquals(finished,true);
        //assert endDate != null;
        //assertTrue(endDate.toLocalDateTime().isAfter(now));
    }

    private void createPseudoOracleDB() {
        Statement statement;

        try {
            remoteMemConnection = SQLUtils.getDBConnection(
                    ChatCloserTimer.configMap.get(DATABASE_DRIVER),
                    ChatCloserTimer.configMap.get(DATABASE_URL),
                    ChatCloserTimer.configMap.get(DATABASE_USER),
                    ChatCloserTimer.configMap.get(DATABASE_PASSWORD));
            remoteMemConnection.setAutoCommit(true);
            statement = remoteMemConnection.createStatement();
            statement.execute( "DROP ALL OBJECTS;"+
                    "CREATE TABLE INTERACTION (" +
                    "        ID VARCHAR2(16) NOT NULL," +
                    "        STATUS NUMBER(3) NOT NULL," +
                    "        ENTITYTYPEID NUMBER(3) NOT NULL," +
                    "        MEDIATYPEID NVARCHAR2(32) NOT NULL," +
                    "        TYPEID NVARCHAR2(32) NOT NULL," +
                    "        SUBTYPEID NVARCHAR2(32)," +
                    "        EXTERNALID NVARCHAR2(256)," +
                    "        OWNERID NUMBER(10)," +
                    "        CONTACTID VARCHAR2(16)," +
                    "        PARENTID VARCHAR2(16)," +
                    "        STARTDATE DATE NOT NULL," +
                    "        ENDDATE DATE," +
                    "        THREADID VARCHAR2(16)," +
                    "        CATEGORYID VARCHAR2(16)," +
                    "        TIMESHIFT NUMBER(5)," +
                    "        SUBJECT NVARCHAR2(512)," +
                    "        TEXT CLOB," +
                    "        STRUCTUREDTEXT CLOB," +
                    "        STRUCTTEXTMIMETYPE VARCHAR2(256)," +
                    "        THECOMMENT CLOB," +
                    "        TENANTID NUMBER(10) NOT NULL," +
                    "        THREADHASH NUMBER(10)," +
                    "        CANBEPARENT NUMBER(1) NOT NULL," +
                    "        CREATORAPPID NUMBER(10) NOT NULL," +
                    "        QUEUENAME NVARCHAR2(64)," +
                    "        ALLATTRIBUTES BLOB," +
                    "        STRATTRIBUTE1 NVARCHAR2(256)," +
                    "        STRATTRIBUTE2 NVARCHAR2(256)," +
                    "        STRATTRIBUTE3 NVARCHAR2(256)," +
                    "        STRATTRIBUTE4 NVARCHAR2(256)," +
                    "        STRATTRIBUTE5 NVARCHAR2(256)," +
                    "        STRATTRIBUTE6 NVARCHAR2(256)," +
                    "        STRATTRIBUTE7 NVARCHAR2(256)," +
                    "        STRATTRIBUTE8 NVARCHAR2(256)," +
                    "        STRATTRIBUTE9 NVARCHAR2(256)," +
                    "        STRATTRIBUTE10 NVARCHAR2(256)," +
                    "        INTATTRIBUTE1 NUMBER(10)," +
                    "        INTATTRIBUTE2 NUMBER(10)," +
                    "        INTATTRIBUTE3 NUMBER(10)," +
                    "        INTATTRIBUTE4 NUMBER(10)," +
                    "        INTATTRIBUTE5 NUMBER(10)," +
                    "        ISSPAM NUMBER(1)," +
                    "        WEBSAFEEMAILSTATUS NVARCHAR2(32)," +
                    "        ISCATEGORYAPPROVED NUMBER(1)," +
                    "        STOPPEDREASON NVARCHAR2(64)," +
                    "        LANG VARCHAR2(64)," +
                    "        MODIFIEDDATE DATE," +
                    "        SUBTENANTID NUMBER(10)," +
                    "        CONSTRAINT PK_INTERACTION PRIMARY KEY (ID)" +
                    "    );");

            statement.execute(script());
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String script() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        return "INSERT INTO INTERACTION (ID, STATUS, ENTITYTYPEID, MEDIATYPEID, TYPEID, SUBTYPEID, EXTERNALID, OWNERID, CONTACTID, PARENTID, STARTDATE, ENDDATE, THREADID, CATEGORYID, TIMESHIFT, SUBJECT, TEXT, STRUCTUREDTEXT, STRUCTTEXTMIMETYPE, THECOMMENT, TENANTID, THREADHASH, CANBEPARENT, CREATORAPPID, QUEUENAME, STRATTRIBUTE1, STRATTRIBUTE2, STRATTRIBUTE3, STRATTRIBUTE4, STRATTRIBUTE5, STRATTRIBUTE6, STRATTRIBUTE7, STRATTRIBUTE8, STRATTRIBUTE9, STRATTRIBUTE10, INTATTRIBUTE1, INTATTRIBUTE2, INTATTRIBUTE3, INTATTRIBUTE4, INTATTRIBUTE5, ISSPAM, WEBSAFEEMAILSTATUS, ISCATEGORYAPPROVED, STOPPEDREASON, LANG, MODIFIEDDATE, SUBTENANTID) "+
                "VALUES ('00027aDEQEY00U32', 3, 2, 'chat', 'Inbound', 'InboundNew', '02BD5B88B2A61A95', 2966, '0001UaCD608W0CFA', null, TIMESTAMP '"+
                now.format(formatter)+"', TIMESTAMP '"+now.plusMinutes(5).format(formatter)+"', '00027aDEQEY00U36', null, 720, ' - Spark Ref: 180831-100011', null, '<?xml version=\"1.0\"?><chatTranscript startAt=\"2018-08-31T03:14:46Z\" sessionId=\"00027aDEQEY00U32\"><newParty userId=\"02BD5B88B2A61A94\" timeShift=\"0\" visibility=\"ALL\" eventId=\"1\"><userInfo personId=\"\" userNick=\"Spark Customer\" userType=\"CLIENT\" protocolType=\"FLEX\" timeZoneOffset=\"0\"/><userData><item key=\"BroadbandDiagnosticJourney\"></item><item key=\"BroadbandDiagnosticServiceID\"></item><item key=\"ConnectionID\">02c702bea8325366</item><item key=\"FirstName\">Spark</item><item key=\"GCTI_LanguageCode\">en-NZ</item><item key=\"IPaddress\">10.235.8.19</item><item key=\"IdentifyCreateContact\">3</item><item key=\"LastName\">Customer</item><item key=\"MediaType\">chat</item><item key=\"PageTag\">PersonalContactUs</item><item key=\"TimeZone\">780</item><item key=\"UserAgent\">Mozilla/5.0 (Windows NT 10.0; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0</item><item key=\"VHQueue\"></item><item key=\"_genesys_pageTitle\">Contact us | Spark NZ</item><item key=\"_genesys_referrer\"></item><item key=\"_genesys_source\">web</item><item key=\"_genesys_url\">https://www-staging.spark.co.nz/contactus/</item></userData></newParty><newParty userId=\"02BD5B7CDBA71995\" timeShift=\"5\" visibility=\"ALL\" eventId=\"2\"><userInfo personId=\"\" userNick=\"Spark&apos;s Virtual Assistant\" userType=\"AGENT\" protocolType=\"BASIC\" timeZoneOffset=\"0\"/></newParty><message userId=\"02BD5B7CDBA71995\" timeShift=\"6\" visibility=\"ALL\" eventId=\"3\"><msgText>Hi, I&apos;m Spark&apos;s chat bot! You can ask me questions like &quot;how do I find my account number&quot; or &quot;how do I set up Netflix&quot;. </msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"6\" visibility=\"ALL\" eventId=\"4\"><noticeText noticeType=\"USER_CUSTOM\">Hi, I&apos;m Spark&apos;s chat bot! You can ask me questions like &quot;how do I find my account number&quot; or &quot;how do I set up Netflix&quot;. </noticeText></notice><message userId=\"02BD5B7CDBA71995\" timeShift=\"6\" visibility=\"ALL\" eventId=\"5\"><msgText>If we get stuck, I can get you through to the team on Live Chat.</msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"6\" visibility=\"ALL\" eventId=\"6\"><noticeText noticeType=\"USER_CUSTOM\">If we get stuck, I can get you through to the team on Live Chat.</noticeText></notice><message userId=\"02BD5B88B2A61A94\" timeShift=\"22\" visibility=\"ALL\" eventId=\"8\"><msgText msgType=\"text\">hey spark how do i set up netflix</msgText></message><message userId=\"02BD5B7CDBA71995\" timeShift=\"23\" visibility=\"ALL\" eventId=\"9\"><msgText>To get Netflix on us, sign in to MySpark using the link I&apos;ve provided. You&apos;ll see the options available for you. If you&apos;re already eligible for Netflix, follow the instructions to get set up." +
                "" +
                "    If you pay for Netflix currently, Get started with Netflix has some handy info. More Information:" +
                "    Manage Netflix in MySpark - https://www-staging.spark.co.nz/secure/myspark/netflix" +
                "    Get started with Netflix - https://www-staging.spark.co.nz/help/get-more/netflix/set-up-netflix/</msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"23\" visibility=\"ALL\" eventId=\"10\"><noticeText noticeType=\"USER_CUSTOM\">To get Netflix on us, sign in to MySpark using the link I&apos;ve provided. You&apos;ll see the options available for you. If you&apos;re already eligible for Netflix, follow the instructions to get set up." +
                "" +
                "    If you pay for Netflix currently, Get started with Netflix has some handy info.&lt;br&gt;&lt;ul class=&quot;gwc-action-list&quot;&gt;&lt;li class=&quot;gwc-action-item&quot;&gt;&lt;a href=&quot;https://www-staging.spark.co.nz/secure/myspark/netflix&quot; title=&quot;Manage Netflix in MySpark&quot; target=&quot;_blank&quot; id=&quot;b0e26d35-ce1b-4bdc-afd4-0e0a61960c60&quot;&gt;Manage Netflix in MySpark&lt;/a&gt;&lt;/li&gt;&lt;li class=&quot;gwc-action-item&quot;&gt;&lt;a href=&quot;https://www-staging.spark.co.nz/help/get-more/netflix/set-up-netflix/&quot; title=&quot;Get started with Netflix&quot; target=&quot;_blank&quot; id=&quot;b0e26d35-ce1b-4bdc-afd4-0e0a61960c60&quot;&gt;Get started with Netflix&lt;/a&gt;&lt;/li&gt;&lt;/ul&gt;</noticeText></notice><message userId=\"02BD5B88B2A61A94\" timeShift=\"29\" visibility=\"ALL\" eventId=\"12\"><msgText msgType=\"text\">that didnt work</msgText></message><message userId=\"02BD5B7CDBA71995\" timeShift=\"31\" visibility=\"ALL\" eventId=\"13\"><msgText>I&apos;m sorry, I&apos;m not sure what you&apos;re asking me. \uD83D\uDE41 Have a go at rephrasing your question or using different words. You can also type: human please if you&apos;d like to talk to one of my human colleagues.</msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"31\" visibility=\"ALL\" eventId=\"14\"><noticeText noticeType=\"USER_CUSTOM\">I&apos;m sorry, I&apos;m not sure what you&apos;re asking me. \uD83D\uDE41 Have a go at rephrasing your question or using different words. You can also type: human please if you&apos;d like to talk to one of my human colleagues.</noticeText></notice><message userId=\"02BD5B88B2A61A94\" timeShift=\"37\" visibility=\"ALL\" eventId=\"16\"><msgText msgType=\"text\">can i talk to a human</msgText></message><message userId=\"02BD5B7CDBA71995\" timeShift=\"39\" visibility=\"ALL\" eventId=\"17\"><msgText>My human colleagues are available to talk to you, they&apos;re really busy at the moment so the wait time may be longer than usual. Select Connect to Live Chat to get started. More Information:" +
                "    Connect to Live Chat</msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"39\" visibility=\"ALL\" eventId=\"18\"><noticeText noticeType=\"USER_CUSTOM\">My human colleagues are available to talk to you, they&apos;re really busy at the moment so the wait time may be longer than usual. Select Connect to Live Chat to get started.&lt;br&gt;&lt;ul class=&quot;gwc-action-list&quot;&gt;&lt;li class=&quot;gwc-action-item&quot;&gt;&lt;a href=&quot;#&quot; class=&quot;gwc-action-text&quot; id=&quot;6a0c3d32-67cb-48d8-92a4-a1846174bb75&quot;&gt;Connect to Live Chat&lt;/a&gt;&lt;/li&gt;&lt;/ul&gt;</noticeText></notice><message userId=\"02BD5B88B2A61A94\" timeShift=\"42\" visibility=\"ALL\" eventId=\"19\"><msgText msgType=\"text\">Connect to Live Chat</msgText></message><message userId=\"02BD5B7CDBA71995\" timeShift=\"44\" visibility=\"ALL\" eventId=\"20\"><msgText>Someone should be available to help you in less than a minute.</msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"44\" visibility=\"ALL\" eventId=\"21\"><noticeText noticeType=\"USER_CUSTOM\">Someone should be available to help you in less than a minute.</noticeText></notice><message userId=\"02BD5B7CDBA71995\" timeShift=\"44\" visibility=\"ALL\" eventId=\"22\"><msgText>Before I transfer you, can you please tell me your name?</msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"44\" visibility=\"ALL\" eventId=\"23\"><noticeText noticeType=\"USER_CUSTOM\">Before I transfer you, can you please tell me your name?</noticeText></notice><message userId=\"02BD5B88B2A61A94\" timeShift=\"54\" visibility=\"ALL\" eventId=\"25\"><msgText msgType=\"text\">Victoria</msgText></message><message userId=\"02BD5B7CDBA71995\" timeShift=\"54\" visibility=\"ALL\" eventId=\"26\"><msgText>Can I have your email address so I can send you a transcript of our chat?</msgText></message><notice userId=\"02BD5B7CDBA71995\" timeShift=\"54\" visibility=\"ALL\" eventId=\"27\"><noticeText noticeType=\"USER_CUSTOM\">Can I have your email address so I can send you a transcript of our chat?</noticeText></notice><message userId=\"02BD5B88B2A61A94\" timeShift=\"60\" visibility=\"ALL\" eventId=\"29\"><msgText msgType=\"text\">victoria.fitch@spark.co.nz</msgText></message><partyLeft userId=\"02BD5B7CDBA71995\" timeShift=\"60\" visibility=\"ALL\" eventId=\"30\" askerId=\"02BD5B7CDBA71995\"><reason>left</reason></partyLeft><newParty userId=\"02BD5B88B2E51A96\" timeShift=\"63\" visibility=\"ALL\" eventId=\"31\"><userInfo personId=\"\" userNick=\"Spark\" userType=\"EXTERNAL\" protocolType=\"ESP\" timeZoneOffset=\"0\"/></newParty><notice userId=\"02BD5B88B2E51A96\" timeShift=\"63\" visibility=\"ALL\" eventId=\"32\"><noticeText noticeType=\"USER_CUSTOM\">switching from bot to real agent</noticeText></notice><message userId=\"02BD5B88B2E51A96\" timeShift=\"63\" visibility=\"ALL\" eventId=\"33\"><msgText>*#You&apos;re being transferred to the Live Chat team. We estimate someone should be with you in less than a minute.</msgText></message><newParty userId=\"02BD5B88B3131A97\" timeShift=\"109\" visibility=\"ALL\" eventId=\"34\"><userInfo personId=\"9876\" userNick=\"Jesus\" userType=\"AGENT\" protocolType=\"BASIC\" timeZoneOffset=\"720\"/></newParty><message userId=\"02BD5B88B3131A97\" timeShift=\"121\" visibility=\"ALL\" eventId=\"36\"><msgText treatAs=\"NORMAL\">Hi</msgText></message><message userId=\"02BD5B88B3131A97\" timeShift=\"128\" visibility=\"ALL\" eventId=\"38\"><msgText treatAs=\"NORMAL\">so you cant setup netflix</msgText></message><message userId=\"02BD5B88B3131A97\" timeShift=\"134\" visibility=\"ALL\" eventId=\"40\"><msgText treatAs=\"NORMAL\">lets take a look at that</msgText></message><message userId=\"02BD5B88B2A61A94\" timeShift=\"146\" visibility=\"ALL\" eventId=\"42\"><msgText msgType=\"text\">sweet thanks</msgText></message><message userId=\"02BD5B88B3131A97\" timeShift=\"183\" visibility=\"ALL\" eventId=\"44\"><msgText treatAs=\"NORMAL\">so all done - but there looks like there is a problem with your internet ill xfer you to our Resolve guys to take a look</msgText></message><partyLeft userId=\"02BD5B88B3131A97\" timeShift=\"192\" visibility=\"ALL\" eventId=\"45\" askerId=\"02BD5B88B3131A97\"><reason>left</reason></partyLeft><message userId=\"02BD5B88B2E51A96\" timeShift=\"195\" visibility=\"ALL\" eventId=\"46\"><msgText>Transferring. Please wait...</msgText></message><newParty userId=\"02BD5B88B3881A98\" timeShift=\"226\" visibility=\"ALL\" eventId=\"47\"><userInfo personId=\"5330\" userNick=\"Vix\" userType=\"AGENT\" protocolType=\"BASIC\" timeZoneOffset=\"720\"/></newParty><message userId=\"02BD5B88B3881A98\" timeShift=\"249\" visibility=\"ALL\" eventId=\"49\"><msgText treatAs=\"NORMAL\">taking a look at your line now</msgText></message><message userId=\"02BD5B88B3881A98\" timeShift=\"259\" visibility=\"ALL\" eventId=\"51\"><msgText treatAs=\"NORMAL\">hmm i think this can be solved by restarting your modem</msgText></message><message userId=\"02BD5B88B2A61A94\" timeShift=\"267\" visibility=\"ALL\" eventId=\"53\"><msgText msgType=\"text\">for sure - ill do that now</msgText></message><message userId=\"02BD5B88B2A61A94\" timeShift=\"268\" visibility=\"ALL\" eventId=\"55\"><msgText msgType=\"text\">thanks</msgText></message><message userId=\"02BD5B88B3881A98\" timeShift=\"296\" visibility=\"ALL\" eventId=\"60\"><msgText treatAs=\"NORMAL\">welcome </msgText></message><partyLeft userId=\"02BD5B88B3881A98\" timeShift=\"299\" visibility=\"ALL\" eventId=\"61\" askerId=\"02BD5B88B3881A98\"><reason code=\"1\">left with request to close if no agents</reason></partyLeft><partyLeft userId=\"02BD5B88B2A61A94\" timeShift=\"299\" visibility=\"ALL\" eventId=\"62\" askerId=\"02BD5B88B3881A98\"><reason code=\"4\">removed by other party</reason></partyLeft></chatTranscript>', 'text/xml', 'Placed in the queue on 31/08/2018 15:17:58 by t462128 - ', 101, 81630065, 1, 701, null, 'victoria.fitch@spark.co.nz', '180831-100011', null, null, null, 'https://www-staging.spark.co.nz/contactus/', null, null, null, null, null, null, null, null, null, null, null, null, 'Normal', null, null, null);";
    }

    private Connection createLocalDB() throws SQLException, ClassNotFoundException {
        Connection connection;
        Statement statement;

        connection = SQLUtils.getDBConnection(
                ChatCloserTimer.configMap.get("database.local.driver"),
                ChatCloserTimer.configMap.get("database.local.url"),
                ChatCloserTimer.configMap.get("database.local.user"),
                ChatCloserTimer.configMap.get("database.local.password"));
        connection.setAutoCommit(true);
        statement = connection.createStatement();
        statement.execute("CREATE SEQUENCE IF NOT EXISTS INTERACTION_SEQ;");
        statement.execute("CREATE TABLE IF NOT EXISTS interaction(id CHAR(18) PRIMARY KEY,sequence BIGINT DEFAULT INTERACTION_SEQ.NEXTVAL,type CHAR(20),startdate TIMESTAMP,enddate TIMESTAMP,timeshift INT DEFAULT 0,xml CLOB, finished BOOLEAN DEFAULT false);");
        statement.execute("CREATE TABLE IF NOT EXISTS token(api CHAR(20) PRIMARY KEY,retrievaldate TIMESTAMP,expirationdate TIMESTAMP,accesstoken VARCHAR(128),refreshtoken VARCHAR(128));");
        statement.execute("CREATE INDEX IF NOT EXISTS date_index_interaction ON interaction(startdate);");
        statement.execute("CREATE INDEX IF NOT EXISTS seq_index_interaction ON interaction(sequence);");
        return connection;
    }

    private void createMockData() throws SQLException {
        final String INSERT_LOCAL = "INSERT INTO interaction (id,type,startdate,enddate,xml,finished) VALUES (?,?,?,?,?,?);";
        final PreparedStatement preparedStatement = localMemConnection.prepareStatement(INSERT_LOCAL);
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

        preparedStatement.setString(1,"00027aDEQEY00U32");
        preparedStatement.setString(2, ContactType.Chat.toString());
        preparedStatement.setTimestamp(3, Timestamp.valueOf(now));
        preparedStatement.setNull(4, Types.TIMESTAMP);
        preparedStatement.setCharacterStream(5, new StringReader(xml), xml.length());
        preparedStatement.setBoolean(6,false);
        preparedStatement.executeUpdate();
    }

    @After
    public void tearDown() throws SQLException {
        localMemConnection.close();
        remoteMemConnection.close();
    }
}
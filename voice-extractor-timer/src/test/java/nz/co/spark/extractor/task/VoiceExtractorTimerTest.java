package nz.co.spark.extractor.task;

import nz.co.spark.cg.shared.sql.SQLUtils;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.model.TriggerType;
import org.junit.*;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class VoiceExtractorTimerTest {
    private static final Logger log = LoggerFactory.getLogger(VoiceExtractorTimerTest.class);
    private static final String VOICE_EXTRACTOR_TIMER_PROPERTIES = "/voice-extractor-timer.properties";
    private VoiceExtractorTimer voiceExtractorTimer;
    private static ClientAndServer mockServer;
    private Connection localMemConnection = null;
    private Properties properties = new Properties();

    @BeforeClass
    public static void start() {
        mockServer = ClientAndServer.startClientAndServer(1011);
    }

    public VoiceExtractorTimerTest() throws ConfigurationException, IOException {
        FileInputStream fis;
        voiceExtractorTimer = new VoiceExtractorTimer();
        properties.load(fis = new FileInputStream(System.getProperty("BASE_DIRECTORY")+"\\config\\"+VOICE_EXTRACTOR_TIMER_PROPERTIES));
        fis.close();
    }

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        log.debug("Starting tests...");
        localMemConnection = createLocalDB();
    }

    @Test
    public void checkTimerConfig() {
        assertEquals(voiceExtractorTimer.getTriggerType(), TriggerType.CRON_EXPRESSION_TRIGGER);
        assertEquals(voiceExtractorTimer.cronExpression(),"0 0/2 * ? * * *");
        assertEquals(voiceExtractorTimer.getJobClass(),VoiceExtractorJob.class);
        assertTrue(voiceExtractorTimer.isConfigEnabled());
        assertEquals(voiceExtractorTimer.getName(),"voice-extractor-timer");
    }

    @Test
    public void checkVoiceExtractorJob() throws SQLException {
        VoiceExtractorJob voiceExtractorJob;
        String sql = "SELECT COUNT(*) FROM interaction WHERE type='Voice';";
        Statement statement = localMemConnection.createStatement();
        int count = 0;

        // Mock configuration //
        new MockServerClient("localhost",1011)
            .when(
                request()
                .withMethod("POST")
                .withPath("/au/v1/token")
                .withBody("client_id=k2gss8wenaxvnvhss7v5xry8&client_secret=mZEE4QMeqd&username=F3803768144&password=EtbHLzypyJSb6UpsWcYd&grant_type=password")
            )
            .respond(
                response()
                .withStatusCode(200)
                .withBody("{\"return_type\":\"json\",\"access_token\":\"ujhwbt2gf3aac42kvzrng2ep\",\"token_type\":\"bearer\",\"expires_in\":86400,\"refresh_token\":\"wwr8k53fvu57w28kn85bwyu8\",\"scope\":null,\"state\":null,\"uri\":null,\"extended\":null}")
                .withHeaders(new Header("Content-Type","application/json"))
            );
        voiceExtractorJob = new VoiceExtractorJob();

        // 4 records
        new MockServerClient("localhost",1011)
            .when(
                request()
                .withMethod("GET")
                .withPath("/au/v1/accounts/spark/recordings")
                .withHeader("Authorization","Bearer ujhwbt2gf3aac42kvzrng2ep")
                .withQueryStringParameter(new Parameter("count","500"))
            )
            .respond(
                response()
                .withStatusCode(200)
                .withBody("{\"self\":\"https://api.dubber.net/au/v1/accounts/spark/recordings\",\"recordings\":[{\"self\":\"https://api.dubber.net/au/v1/recordings/1457439471\",\"id\":\"1457439471\",\"to\":\"0800000000\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Mon, 03 Dec 2018 10:48:42 +1300\",\"duration\":5,\"expiry_time\":\"Sun, 03 Mar 2019 10:48:55 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0800000000_+6494702494_20181202T094842+0000_ca0edf1c-6160-8f2d-f55a-e6c9196dd1b2.wav\",\"external-tracking-ids\":\"97aa08b5-3d74-40a4-a8a3-01b354ca19de\",\"recorder-call-id\":\"BW2148428970212182013987012@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au2\",\"external-call-id\":\"callhalf-6242239189:0\"},\"date_created\":\"Sun, 02 Dec 2018 21:48:55 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:33 +0000\"},{\"self\":\"https://api.dubber.net/au/v1/recordings/2533637993\",\"id\":\"2533637993\",\"to\":\"0272000295\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Mon, 03 Dec 2018 09:15:52 +1300\",\"duration\":24,\"expiry_time\":\"Sun, 03 Mar 2019 09:16:17 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0272000295_+6494702494_20181202T081552+0000_9a6db27a-2c12-6fa8-0301-c972b9e92892.wav\",\"external-tracking-ids\":\"c1d5017e-a1a0-45dc-8bc1-8e421adcef5c\",\"recorder-call-id\":\"BW201552416021218-274451700@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au1\",\"external-call-id\":\"callhalf-6240312101:0\"},\"date_created\":\"Sun, 02 Dec 2018 20:16:17 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:34 +0000\"},{\"self\":\"https://api.dubber.net/au/v1/recordings/0519021328\",\"id\":\"0519021328\",\"to\":\"0800000000\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Mon, 03 Dec 2018 09:11:50 +1300\",\"duration\":12,\"expiry_time\":\"Sun, 03 Mar 2019 09:12:04 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0800000000_+6494702494_20181202T081150+0000_e71d5a97-5b7b-9354-2278-c431a1e17124.wav\",\"external-tracking-ids\":\"d4309c08-7a64-4ead-837f-7c654e8d2d16\",\"recorder-call-id\":\"BW201150793021218-1779833003@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au1\",\"external-call-id\":\"callhalf-6240238995:0\"},\"date_created\":\"Sun, 02 Dec 2018 20:12:04 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:33 +0000\"},{\"self\":\"https://api.dubber.net/au/v1/recordings/1660548120\",\"id\":\"1660548120\",\"to\":\"0800000000\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Sun, 02 Dec 2018 19:13:27 +1300\",\"duration\":21,\"expiry_time\":\"Sat, 02 Mar 2019 19:13:51 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0800000000_+6494702494_20181202T061327+0000_38ba7a6c-0d3d-b8ed-f40c-820b2eae787c.wav\",\"external-tracking-ids\":\"c59da411-1f47-429b-9939-2f165212e940\",\"recorder-call-id\":\"BW061327568021218100202269@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au1\",\"external-call-id\":\"callhalf-6226512043:0\"},\"date_created\":\"Sun, 02 Dec 2018 06:13:51 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:34 +0000\"}]}")
                .withHeaders(new Header("Content-Type","application/json"))
            );

        new MockServerClient("localhost",1011)
            .when(
                request()
                .withMethod("GET")
                .withPath("/au/v1/recordings/1457439471")
                .withHeader("Authorization","Bearer ujhwbt2gf3aac42kvzrng2ep")
                .withQueryStringParameter("listener","rod.dinis@spark.co.nz")
            )
            .respond(
                response()
                .withStatusCode(200)
                .withBody("{\"self\":\"https://api.dubber.net/au/v1/recordings/1457439471\",\"account\":\"https://api.dubber.net/au/v1/accounts/spark\",\"id\":\"1457439471\",\"to\":\"0800000000\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Mon, 03 Dec 2018 10:48:42 +1300\",\"duration\":5,\"expiry_time\":\"Sun, 03 Mar 2019 10:48:55 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0800000000_+6494702494_20181202T094842+0000_ca0edf1c-6160-8f2d-f55a-e6c9196dd1b2.wav\",\"external-tracking-ids\":\"97aa08b5-3d74-40a4-a8a3-01b354ca19de\",\"recorder-call-id\":\"BW2148428970212182013987012@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au2\",\"external-call-id\":\"callhalf-6242239189:0\"},\"date_created\":\"Sun, 02 Dec 2018 21:48:55 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:33 +0000\",\"recording_url\":\"http://localhost:1011/F3803768144/1457439471.mp3\"}")
                .withHeaders(new Header("Content-Type","application/json"))
            );

        new MockServerClient("localhost",1011)
                .when(
                    request()
                    .withMethod("GET")
                    .withPath("/au/v1/recordings/2533637993")
                    .withHeader("Authorization","Bearer ujhwbt2gf3aac42kvzrng2ep")
                    .withQueryStringParameter("listener","rod.dinis@spark.co.nz")
                )
                .respond(
                    response()
                    .withStatusCode(200)
                    .withBody("{\"self\":\"https://api.dubber.net/au/v1/recordings/2533637993\",\"account\":\"https://api.dubber.net/au/v1/accounts/spark\",\"id\":\"2533637993\",\"to\":\"0800000000\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Mon, 03 Dec 2018 10:48:42 +1300\",\"duration\":5,\"expiry_time\":\"Sun, 03 Mar 2019 10:48:55 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0800000000_+6494702494_20181202T094842+0000_ca0edf1c-6160-8f2d-f55a-e6c9196dd1b2.wav\",\"external-tracking-ids\":\"97aa08b5-3d74-40a4-a8a3-01b354ca19de\",\"recorder-call-id\":\"BW2148428970212182013987012@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au2\",\"external-call-id\":\"callhalf-6242239189:0\"},\"date_created\":\"Sun, 02 Dec 2018 21:48:55 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:33 +0000\",\"recording_url\":\"http://localhost:1011/F3803768144/1457439471.mp3\"}")
                    .withHeaders(new Header("Content-Type","application/json"))
                );

        new MockServerClient("localhost",1011)
                .when(
                    request()
                    .withMethod("GET")
                    .withPath("/au/v1/recordings/0519021328")
                    .withHeader("Authorization","Bearer ujhwbt2gf3aac42kvzrng2ep")
                    .withQueryStringParameter("listener","rod.dinis@spark.co.nz")
                )
                .respond(
                    response()
                    .withStatusCode(200)
                    .withBody("{\"self\":\"https://api.dubber.net/au/v1/recordings/0519021328\",\"account\":\"https://api.dubber.net/au/v1/accounts/spark\",\"id\":\"0519021328\",\"to\":\"0800000000\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Mon, 03 Dec 2018 10:48:42 +1300\",\"duration\":5,\"expiry_time\":\"Sun, 03 Mar 2019 10:48:55 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0800000000_+6494702494_20181202T094842+0000_ca0edf1c-6160-8f2d-f55a-e6c9196dd1b2.wav\",\"external-tracking-ids\":\"97aa08b5-3d74-40a4-a8a3-01b354ca19de\",\"recorder-call-id\":\"BW2148428970212182013987012@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au2\",\"external-call-id\":\"callhalf-6242239189:0\"},\"date_created\":\"Sun, 02 Dec 2018 21:48:55 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:33 +0000\",\"recording_url\":\"http://localhost:1011/F3803768144/1457439471.mp3\"}")
                    .withHeaders(new Header("Content-Type","application/json"))
                );

        new MockServerClient("localhost",1011)
                .when(
                    request()
                    .withMethod("GET")
                    .withPath("/au/v1/recordings/1660548120")
                    .withHeader("Authorization","Bearer ujhwbt2gf3aac42kvzrng2ep")
                    .withQueryStringParameter("listener","rod.dinis@spark.co.nz")
                )
                .respond(
                    response()
                    .withStatusCode(200)
                    .withBody("{\"self\":\"https://api.dubber.net/au/v1/recordings/1660548120\",\"account\":\"https://api.dubber.net/au/v1/accounts/spark\",\"id\":\"1660548120\",\"to\":\"0800000000\",\"from\":\"+6494702494\",\"from_label\":\"Aaron Redwood\",\"call_type\":\"outbound\",\"recording_type\":\"recording\",\"channel\":\"Aaron Redwood\",\"status\":\"Active\",\"start_time\":\"Mon, 03 Dec 2018 10:48:42 +1300\",\"duration\":5,\"expiry_time\":\"Sun, 03 Mar 2019 10:48:55 +1300\",\"dub_point_id\":\"5db59cbabeffc375151b\",\"meta_tags\":{\"recording-platform\":\"broadworks\",\"original-filename\":\"dub_0800000000_+6494702494_20181202T094842+0000_ca0edf1c-6160-8f2d-f55a-e6c9196dd1b2.wav\",\"external-tracking-ids\":\"97aa08b5-3d74-40a4-a8a3-01b354ca19de\",\"recorder-call-id\":\"BW2148428970212182013987012@10.70.16.156\",\"recorder-identifier\":\"ap-southeast-2.dr-bc-au2\",\"external-call-id\":\"callhalf-6242239189:0\"},\"date_created\":\"Sun, 02 Dec 2018 21:48:55 +0000\",\"date_updated\":\"Mon, 03 Dec 2018 03:57:33 +0000\",\"recording_url\":\"http://localhost:1011/F3803768144/1457439471.mp3\"}")
                    .withHeaders(new Header("Content-Type","application/json"))
                );

        new MockServerClient("localhost",1011)
            .when(
                request()
                .withMethod("GET")
                .withPath("/F3803768144/1457439471.mp3")
            )
            .respond(
                response()
                .withStatusCode(200)
                .withBody("{THIS IS AN AUDIO FILE}")
                .withHeaders(new Header("Content-Type","audio/mpeg"))
            );

        voiceExtractorJob.execute(null);
        ResultSet resultSet = statement.executeQuery(sql);
        if( resultSet.next() ) {
            count = resultSet.getInt(1);
        }
        assertEquals(count,4);
        statement.close();
    }

    private Connection createLocalDB() throws SQLException, ClassNotFoundException {
        Connection connection;
        Statement statement;

        connection = SQLUtils.getDBConnection(
            VoiceExtractorTimer.configMap.get("database.local.driver"),
            VoiceExtractorTimer.configMap.get("database.local.url"),
            VoiceExtractorTimer.configMap.get("database.local.user"),
            VoiceExtractorTimer.configMap.get("database.local.password")
        );
        connection.setAutoCommit(true);
        statement = connection.createStatement();
        statement.execute("CREATE SEQUENCE IF NOT EXISTS INTERACTION_SEQ;");
        statement.execute("CREATE TABLE IF NOT EXISTS interaction(id CHAR(18),sequence BIGINT DEFAULT INTERACTION_SEQ.NEXTVAL,type CHAR(20),startdate TIMESTAMP,enddate TIMESTAMP,timeshift INT DEFAULT 0,xml CLOB, finished BOOLEAN DEFAULT false, PRIMARY KEY(ID,type));");
        statement.execute("CREATE TABLE IF NOT EXISTS token(api CHAR(20) PRIMARY KEY,retrievaldate TIMESTAMP,expirationdate TIMESTAMP,accesstoken VARCHAR(128),refreshtoken VARCHAR(128));");
        statement.execute("CREATE INDEX IF NOT EXISTS date_index_interaction ON interaction(startdate);");
        statement.execute("CREATE INDEX IF NOT EXISTS seq_index_interaction ON interaction(sequence);");
        return connection;
    }

    @After
    public void tearDown() {
        try {
            if( localMemConnection != null ) {
                localMemConnection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void stop() {
        mockServer.stop();
    }
}
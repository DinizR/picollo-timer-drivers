package nz.co.spark.cg.cleaner.task;

import nz.co.spark.cg.shared.model.ContactType;
import nz.co.spark.cg.shared.sql.SQLUtils;
import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.model.TriggerType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

public class VoiceCleanerJobTest {
    private Connection memConnection = null;
    private VoiceCleanerTimer voiceDBCleanerTimer;

    @Before
    public void setup() throws ConfigurationException, SQLException, ClassNotFoundException {
        voiceDBCleanerTimer = new VoiceCleanerTimer();
        voiceDBCleanerTimer.loadConfig();
        memConnection = createLocalDB();
    }

    @Test
    public void checkCleanerTimerAttributes() {
        assertFalse(voiceDBCleanerTimer.cronExpression().isEmpty());
        assertEquals(voiceDBCleanerTimer.cronExpression(),"0 0/1 * ? * * *");
        assertEquals(voiceDBCleanerTimer.getTriggerType(), TriggerType.CRON_EXPRESSION_TRIGGER);
        assertEquals(voiceDBCleanerTimer.getJobClass(),VoiceCleanerJob.class);
        assertEquals(voiceDBCleanerTimer.getName(),"voice-cleaner-timer");
        assertFalse(voiceDBCleanerTimer.getDescription().isEmpty());
        assertTrue(voiceDBCleanerTimer.isConfigEnabled());
        assertEquals(voiceDBCleanerTimer.getType(), DriverType.Custom);
    }

    @Test
    public void checkCleanerJob() throws SQLException {
        VoiceCleanerJob voiceCleanerJob = new VoiceCleanerJob();
        String sql = "SELECT COUNT(*) FROM interaction WHERE type='Voice';";
        Statement statement = memConnection.createStatement();
        int record = -1;

        createMockData();
        voiceCleanerJob.execute(null);
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
                VoiceCleanerTimer.configMap.get("database.driver"),
                VoiceCleanerTimer.configMap.get("database.url"),
                VoiceCleanerTimer.configMap.get("database.user"),
                VoiceCleanerTimer.configMap.get("database.password"));
        connection.setAutoCommit(true);
        statement = connection.createStatement();
        statement.execute("CREATE SEQUENCE IF NOT EXISTS INTERACTION_SEQ;");
        statement.execute("CREATE TABLE IF NOT EXISTS interaction(id CHAR(18),sequence BIGINT DEFAULT INTERACTION_SEQ.NEXTVAL,type CHAR(20),startdate TIMESTAMP,enddate TIMESTAMP,timeshift INT DEFAULT 0,xml CLOB, finished BOOLEAN DEFAULT false, PRIMARY KEY(ID,type));");
        statement.execute("CREATE TABLE IF NOT EXISTS token(api CHAR(20) PRIMARY KEY,retrievaldate TIMESTAMP,expirationdate TIMESTAMP,accesstoken VARCHAR(128),refreshtoken VARCHAR(128));");
        statement.execute("CREATE INDEX IF NOT EXISTS date_index_interaction ON interaction(startdate);");
        statement.execute("CREATE INDEX IF NOT EXISTS seq_index_interaction ON interaction(sequence);");
        return connection;
    }

    private void createMockData() throws SQLException {
        final String INSERT_LOCAL = "INSERT INTO interaction (id,type,startdate,enddate) VALUES (?,?,?,?);";
        final PreparedStatement preparedStatement = memConnection.prepareStatement(INSERT_LOCAL);
        final LocalDateTime yesterday = LocalDateTime.now().minusDays(1);

        preparedStatement.setString(1,"00027aDEQEY00TYB");
        preparedStatement.setString(2, ContactType.Chat.toString());
        preparedStatement.setTimestamp(3, Timestamp.valueOf(yesterday));
        preparedStatement.setNull(4, Types.TIMESTAMP);
        preparedStatement.executeUpdate();

        preparedStatement.setString(1,"00028aDEQEY00TYB");
        preparedStatement.setString(2, ContactType.Chat.toString());
        preparedStatement.setTimestamp(3, Timestamp.valueOf(yesterday));
        preparedStatement.setNull(4, Types.TIMESTAMP);
        preparedStatement.executeUpdate();

        preparedStatement.setString(1,"00029aDEQEY00TYB");
        preparedStatement.setString(2, ContactType.Chat.toString());
        preparedStatement.setTimestamp(3, Timestamp.valueOf(yesterday));
        preparedStatement.setNull(4, Types.TIMESTAMP);
        preparedStatement.executeUpdate();
    }

    @After
    public void tearDown() throws SQLException {
        if( memConnection != null )
            memConnection.close();
    }
}
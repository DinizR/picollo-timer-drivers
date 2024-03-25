/*
* VoiceCleanerJob.java
 */
package nz.co.spark.cg.cleaner.task;

import nz.co.spark.cg.shared.sql.SQLUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Voice Cleaner Job.
 * @author rod
 * @since 2018-12-06
 */
@DisallowConcurrentExecution
public class VoiceCleanerJob implements Job {

    public VoiceCleanerJob() {
        SQLUtils.createLocalDB(
                VoiceCleanerTimer.configMap.get("database.driver"),
                VoiceCleanerTimer.configMap.get("database.url"),
                VoiceCleanerTimer.configMap.get("database.user"),
                VoiceCleanerTimer.configMap.get("database.password")
        );
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        Connection connection = null;
        PreparedStatement statement;
        Optional<String> id;

        try {
            VoiceCleanerTimer.logger.info("Running Voice DB cleaner task... ");
            connection = SQLUtils.getDBConnection(
                    VoiceCleanerTimer.configMap.get("database.driver"),
                    VoiceCleanerTimer.configMap.get("database.url"),
                    VoiceCleanerTimer.configMap.get("database.user"),
                    VoiceCleanerTimer.configMap.get("database.password"));
            id = SQLUtils.fetchLastId(connection, "SELECT id FROM interaction WHERE type = 'Voice' ORDER BY sequence DESC LIMIT 1;");

            if( id.isPresent() ) {
                statement = connection.prepareStatement("DELETE FROM interaction WHERE type = 'Voice' AND startdate < ? AND id <> ?");
                statement.setString(2, id.get());
            } else {
                statement = connection.prepareStatement("DELETE FROM interaction WHERE type = 'Voice' AND startdate < ?");
            }
            statement.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().minusDays(7L)));
            statement.executeUpdate();
            connection.commit();
            VoiceCleanerTimer.logger.info("Voice DB cleaner task finished.");
        } catch ( Throwable e ) {
            VoiceCleanerTimer.logger.error("Problems running task.",e);
            if( connection != null ) {
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                }
            }
        } finally {
            if( connection != null ) {
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
    }
}
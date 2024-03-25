/*
* ChatCleanerJob.java
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
 * Chat Cleaner Job.
 * @author rod
 * @since 2018-09-13
 */
@DisallowConcurrentExecution
public class ChatCleanerJob implements Job {

    public ChatCleanerJob() {
        SQLUtils.createLocalDB(
                ChatCleanerTimer.configMap.get("database.driver"),
                ChatCleanerTimer.configMap.get("database.url"),
                ChatCleanerTimer.configMap.get("database.user"),
                ChatCleanerTimer.configMap.get("database.password")
        );
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        Connection connection = null;
        PreparedStatement statement;
        Optional<String> id;

        try {
            ChatCleanerTimer.logger.info("Running Chat DB cleaner task... ");
            connection = SQLUtils.getDBConnection(
                    ChatCleanerTimer.configMap.get("database.driver"),
                    ChatCleanerTimer.configMap.get("database.url"),
                    ChatCleanerTimer.configMap.get("database.user"),
                    ChatCleanerTimer.configMap.get("database.password"));
            id = SQLUtils.fetchLastId(connection, "SELECT id FROM interaction WHERE type = 'Chat' ORDER BY sequence DESC LIMIT 1;");

            if( id.isPresent() ) {
                statement = connection.prepareStatement("DELETE FROM interaction WHERE type = 'Chat' AND startdate < ? AND id <> ?");
                statement.setString(2, id.get());
            } else {
                statement = connection.prepareStatement("DELETE FROM interaction WHERE type = 'Chat' AND startdate < ?");
            }

            statement.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().minusDays(7L)));
            statement.executeUpdate();
            connection.commit();
            ChatCleanerTimer.logger.info("Chat DB cleaner task finished.");
        } catch ( Throwable e ) {
            ChatCleanerTimer.logger.error("Problems running task.",e);
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
/*
* SocialCleanerJob.java
 */
package nz.co.spark.cg.social.cleaner.task;

import nz.co.spark.cg.shared.sql.SQLUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Social Contact Cleaner Job.
 * @author rod
 * @since 2018-10-05
 */
@DisallowConcurrentExecution
public class SocialCleanerJob implements Job {

    public SocialCleanerJob() {
        SQLUtils.createLocalDB(
            SocialCleanerTimer.configMap.get("database.driver"),
            SocialCleanerTimer.configMap.get("database.url"),
            SocialCleanerTimer.configMap.get("database.user"),
            SocialCleanerTimer.configMap.get("database.password")
        );
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        Connection connection = null;
        PreparedStatement statement;
        Optional<String> id;

        try {
            SocialCleanerTimer.logger.info("Running Social DB cleaner task... ");
            connection = SQLUtils.getDBConnection(
                    SocialCleanerTimer.configMap.get("database.driver"),
                    SocialCleanerTimer.configMap.get("database.url"),
                    SocialCleanerTimer.configMap.get("database.user"),
                    SocialCleanerTimer.configMap.get("database.password"));
            id = SQLUtils.fetchLastId(connection, "SELECT id FROM interaction WHERE type = 'Social' ORDER BY sequence DESC LIMIT 1;");

            if( id.isPresent() ) {
                statement = connection.prepareStatement("DELETE FROM interaction WHERE type = 'Social' AND startdate < ? AND id <> ?");
                statement.setString(2, id.get());
            } else {
                statement = connection.prepareStatement("DELETE FROM interaction WHERE type = 'Social' AND startdate < ?");
            }

            statement.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().minusDays(7L)));
            statement.executeUpdate();
            connection.commit();
            SocialCleanerTimer.logger.info("Social DB cleaner task finished.");
        } catch ( Throwable e ) {
            SocialCleanerTimer.logger.error("Problems running Social DB cleaner task. Message:{}",e);
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

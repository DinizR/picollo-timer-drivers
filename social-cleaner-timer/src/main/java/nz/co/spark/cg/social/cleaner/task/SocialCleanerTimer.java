/*
* SocialCleanerTimer.java
 */
package nz.co.spark.cg.social.cleaner.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Timer module to clean Social records from local database.
* @author rod
* @since 2018-09-12
 */
public class SocialCleanerTimer extends TimerSupplierInterface {
    private static final String EXECUTION_TIMER_CRON = "execution.timer.cron";
    static ch.qos.logback.classic.Logger logger;
    static Map<String,String> configMap;

    public SocialCleanerTimer() throws ConfigurationException {
        SocialCleanerTimer.logger = getComponentLogger();
        SocialCleanerTimer.configMap = getConfigMap();
    }

    public TriggerType getTriggerType() {
        return TriggerType.CRON_EXPRESSION_TRIGGER;
    }

    @Override
    public String cronExpression() {
        return getConfigValue(EXECUTION_TIMER_CRON);
    }

    @Override
    public Class getJobClass() {
        return SocialCleanerJob.class;
    }

    @Override
    public boolean isConfigEnabled() {
        return true;
    }

    @Override
    public DriverType getType() {
        return DriverType.Custom;
    }

    @Override
    public String getName() {
        return "social-cleaner-timer";
    }

    @Override
    public String getDescription() {
        return "This plugin intents to clean local database from social extractions captured from UCS database.";
    }
}
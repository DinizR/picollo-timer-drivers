/*
* VoiceCleanerTimer.java
 */
package nz.co.spark.cg.cleaner.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Module to clean voice transcriptions in local DB.
* @author rod
* @since 2018-12-06
 */
public class VoiceCleanerTimer extends TimerSupplierInterface {
    private static final String EXECUTION_TIMER_CRON = "execution.timer.cron";
    static ch.qos.logback.classic.Logger logger;
    static Map<String,String> configMap;

    public VoiceCleanerTimer() throws ConfigurationException {
        VoiceCleanerTimer.logger = getComponentLogger();
        configMap = getConfigMap();
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
        return VoiceCleanerJob.class;
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
        return "voice-cleaner-timer";
    }

    @Override
    public String getDescription() {
        return "This plugin intents to clean local database from voice extractions captured from Dubber API.";
    }
}
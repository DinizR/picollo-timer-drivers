/*
* ChatExtractorTimer.java
 */
package nz.co.spark.extractor.task;

import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;
import org.picollo.config.api.ConfigurationException;
import org.picollo.driver.DriverType;

import java.util.Map;

/**
* Module to extract chat transcriptions, clean up, and send to be consumed by Big Data platform.
* @author rod
* @since 2018-08-30
 */
public class ChatExtractorTimer extends TimerSupplierInterface {
    private static final String TIMER_CRON = "execution.timer.cron";
    public static ch.qos.logback.classic.Logger logger;
    public static Map<String,String> configMap;

    public ChatExtractorTimer() throws ConfigurationException {
        ChatExtractorTimer.logger = getComponentLogger();
        ChatExtractorTimer.configMap = getConfigMap();
    }

    public TriggerType getTriggerType() {
        return TriggerType.CRON_EXPRESSION_TRIGGER;
    }

    @Override
    public boolean isConfigEnabled() {
        return true;
    }

    @Override
    public String cronExpression() {
        return getConfigValue(TIMER_CRON);
    }

    @Override
    public Class getJobClass() {
        return ChatExtractorJob.class;
    }

    @Override
    public DriverType getType() {
        return DriverType.Custom;
    }

    @Override
    public String getName() {
        return "chat-extractor-timer";
    }

    @Override
    public String getDescription() {
        return "That plugin intents to extract chat conversation transcriptions from UCS database and generate XML files with that transcriptions.";
    }
}
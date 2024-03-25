/*
* ChatCloserTimer.java
 */
package nz.co.spark.cg.closer.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Module to close chat transcriptions that were not finished when extracting and send to be consumed by Big Data platform.
* @author rod
* @since 2018-09-12
 */
public class ChatCloserTimer extends TimerSupplierInterface {
    private static final String EXECUTION_TIMER_CRON = "execution.timer.cron";
    public static ch.qos.logback.classic.Logger logger;
    public static Map<String,String> configMap;

    public ChatCloserTimer() throws ConfigurationException {
        ChatCloserTimer.logger = getComponentLogger();
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
        return ChatCloserJob.class;
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
        return "chat-closer-timer";
    }

    @Override
    public String getDescription() {
        return "This plugin intents to close social contacts that were open when the chat extractor plugin ran.";
    }
}
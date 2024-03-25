/*
* VoiceExtractorTimer.java
 */
package nz.co.spark.extractor.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Module to extract voice files and send to be consumed by Big Data platform.
* @author rod
* @since 2018-12-04
 */
public class VoiceExtractorTimer extends TimerSupplierInterface {
    private static final String TIMER_CRON = "execution.timer.cron";
    static ch.qos.logback.classic.Logger logger;
    static Map<String,String> configMap;

    public VoiceExtractorTimer() throws ConfigurationException {
        VoiceExtractorTimer.logger = getComponentLogger();
        configMap = getConfigMap();
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
        return VoiceExtractorJob.class;
    }

    @Override
    public DriverType getType() {
        return DriverType.Custom;
    }

    @Override
    public String getName() {
        return "voice-extractor-timer";
    }

    @Override
    public String getDescription() {
        return "That plugin intents to extract voice recordings from Dubber cloud platform and deliver to be consumed by Big Data platform.";
    }
}
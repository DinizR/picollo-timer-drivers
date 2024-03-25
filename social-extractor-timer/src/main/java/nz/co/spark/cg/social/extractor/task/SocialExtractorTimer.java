/*
* SocialExtractorTimer.java
 */
package nz.co.spark.cg.social.extractor.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Module to extract social transcriptions, clean up, and send to be consumed by Big Data platform.
* @author rod
* @since 2018-10-04
 */
public class SocialExtractorTimer extends TimerSupplierInterface {
    private static final String TIMER_CRON = "execution.timer.cron";
    public static ch.qos.logback.classic.Logger logger;
    public static Map<String,String> configMap;


    public SocialExtractorTimer() throws ConfigurationException {
        SocialExtractorTimer.logger = getComponentLogger();
        SocialExtractorTimer.configMap = getConfigMap();
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
        return SocialExtractorJob.class;
    }

    @Override
    public DriverType getType() {
        return DriverType.Custom;
    }

    @Override
    public String getName() {
        return "social-extractor-timer";
    }

    @Override
    public String getDescription() {
        return "That plugin intents to extract Social conversation transcriptions from UCS database and generate XML files with that transcriptions.";
    }
}
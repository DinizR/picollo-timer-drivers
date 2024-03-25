/*
* SocialCloserTimer.java
 */
package nz.co.spark.cg.social.closer.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Module to close social contact transcriptions that were not finished when extracting and send to be consumed by Big Data platform.
* @author rod
* @since 2018-10-05
 */
public class SocialCloserTimer extends TimerSupplierInterface {
    private static final String EXECUTION_TIMER_CRON = "execution.timer.cron";
    public static ch.qos.logback.classic.Logger logger;
    public static Map<String,String> configMap;


    public SocialCloserTimer() throws ConfigurationException {
        SocialCloserTimer.logger = getComponentLogger();
        SocialCloserTimer.configMap = getConfigMap();
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
        return SocialCloserJob.class;
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
        return "social-closer-timer";
    }

    @Override
    public String getDescription() {
        return "This plugin intents to close social contacts that were open when the chat extractor plugin ran.";
    }
}
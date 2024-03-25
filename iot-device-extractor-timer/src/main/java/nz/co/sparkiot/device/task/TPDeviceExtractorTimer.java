/*
* UDRExtractorTimer.java
 */
package nz.co.sparkiot.device.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Timer Module to extract ThinkPark Devices to IoT domain.
* @author rod
 * @since 2019-02-18
 */
public class TPDeviceExtractorTimer extends TimerSupplierInterface {
    private static final String TIMER_CRON = "execution.timer.cron";
    static ch.qos.logback.classic.Logger logger;
    static Map<String,String> configMap;

    public TPDeviceExtractorTimer() throws ConfigurationException {
        TPDeviceExtractorTimer.logger = getComponentLogger();
        TPDeviceExtractorTimer.configMap = getConfigMap();
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
        return TPDeviceExtractorJob.class;
    }

    @Override
    public DriverType getType() {
        return DriverType.Custom;
    }

    @Override
    public String getName() {
        return "udr-extractor-timer";
    }

    @Override
    public String getDescription() {
        return "That plugin intents to extract UDR file from Actility platform and generate data on IoT domain.";
    }
}
/*
* LoaderTimer.java
 */
package com.megaball.timer.loader.task;

import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;
import org.picollo.config.api.ConfigurationException;
import org.picollo.driver.DriverType;

import java.util.Map;

/**
* Timer module driver to load Lottery results.
* @author rod
* @since 2021-08
 */
public class LoaderTimer extends TimerSupplierInterface {
    private static final String EXECUTION_TIMER_CRON = "execution.timer.cron";
    static ch.qos.logback.classic.Logger logger;
    static Map<String,String> configMap;

    public LoaderTimer() throws ConfigurationException {
        LoaderTimer.logger = getComponentLogger();
        LoaderTimer.configMap = getConfigMap();
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
        return LototurfLoaderTimerJob.class;
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
        return "lototurf-timer";
    }

    @Override
    public String getDescription() {
        return "This is a timer for load Lototurf results.";
    }
}
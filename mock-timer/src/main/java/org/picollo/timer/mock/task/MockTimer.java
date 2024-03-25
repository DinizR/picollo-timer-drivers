/*
* MockTimer.java
 */
package org.picollo.timer.mock.task;

import org.picollo.config.api.ConfigurationException;
import org.picollo.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;

import java.util.Map;

/**
* Timer module mock a timer driver.
* @author rod
* @since 2019-04
 */
public class MockTimer extends TimerSupplierInterface {
    private static final String EXECUTION_TIMER_CRON = "execution.timer.cron";
    static ch.qos.logback.classic.Logger logger;
    private static Map<String,String> configMap;

    public MockTimer() throws ConfigurationException {
        MockTimer.logger = getComponentLogger();
        MockTimer.configMap = getConfigMap();
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
        return MockTimerJob.class;
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
        return "mock-timer";
    }

    @Override
    public String getDescription() {
        return "This plugin mock a timer for testing purposes.";
    }
}
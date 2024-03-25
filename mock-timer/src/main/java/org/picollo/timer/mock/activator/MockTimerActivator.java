/*
 * MockTimerActivator.java
 */
package org.picollo.timer.mock.activator;

import org.picollo.config.api.ConfigurableInterface;
import org.cobra.timer.api.TimerSupplierInterface;
import org.picollo.timer.mock.task.MockTimer;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver timer mock for testing purposes.
 * @author rod
 * @since 2019-04
 */
public class MockTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(MockTimerActivator.class);
    private static BundleContext context;
    private MockTimer mockTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws org.picollo.config.api.ConfigurationException {
        MockTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, mockTimer = new MockTimer(), null);
        log.info("Mock Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        MockTimerActivator.context = null;
        mockTimer.removeJob();
        log.info("Mock Timer has stopped successfully.");
    }
}
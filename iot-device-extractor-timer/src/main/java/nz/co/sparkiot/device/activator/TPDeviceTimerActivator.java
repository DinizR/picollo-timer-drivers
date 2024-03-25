/*
 * TPDeviceTimerActivator.java
 */
package nz.co.sparkiot.device.activator;

import nz.co.sparkiot.device.task.TPDeviceExtractorTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver activator for ThinkPark devices extractor.
 * @author rod
 * @since 2019-02-18
 */
public class TPDeviceTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(TPDeviceTimerActivator.class);
    private static BundleContext context;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        TPDeviceTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, new TPDeviceExtractorTimer(), null);

        log.info("Actility Device Extractor Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        TPDeviceTimerActivator.context = null;
        log.info("Actility Device Extractor Timer has stopped successfully.");
    }
}
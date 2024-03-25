/*
 * UDRTimerActivator.java
 */
package nz.co.sparkiot.udr.activator;

import nz.co.sparkiot.udr.task.UDRExtractorTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver activator for UDR extractor.
 * @author rod
 * @since 2019-02-13
 */
public class UDRTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(UDRTimerActivator.class);
    private static BundleContext context;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        UDRTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, new UDRExtractorTimer(), null);

        log.info("Actility UDR Extractor Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        UDRTimerActivator.context = null;
        log.info("Actility UDR Extractor Timer has stopped successfully.");
    }
}
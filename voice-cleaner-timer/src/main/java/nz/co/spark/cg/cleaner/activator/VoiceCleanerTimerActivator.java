/*
 * VoiceCleanerTimerActivator.java
 */
package nz.co.spark.cg.cleaner.activator;

import nz.co.spark.cg.cleaner.task.VoiceCleanerTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver timer cleaner for Voice database cleaning.
 * @author rod
 * @since 2018-12-06
 */
public class VoiceCleanerTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(VoiceCleanerTimerActivator.class);
    private static BundleContext context;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws Exception {
        VoiceCleanerTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, new VoiceCleanerTimer(), null);
        log.info("Voice DB Cleaner Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        VoiceCleanerTimerActivator.context = null;
        log.info("Voice DB Cleaner Timer has stopped successfully.");
    }
}
/*
 * SocialCleanerTimerActivator.java
 */
package nz.co.spark.cg.social.cleaner.activator;

import nz.co.spark.cg.social.cleaner.task.SocialCleanerTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver timer cleaner for Social database cleaning.
 * @author rod
 * @since 2018-10-04
 */
public class SocialCleanerTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(SocialCleanerTimerActivator.class);
    private static BundleContext context;
    private SocialCleanerTimer socialCleanerTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        SocialCleanerTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, socialCleanerTimer = new SocialCleanerTimer(), null);
        log.info("Social DB Cleaner Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        SocialCleanerTimerActivator.context = null;
        socialCleanerTimer.removeJob();
        log.info("Social DB Cleaner Timer has stopped successfully.");
    }
}
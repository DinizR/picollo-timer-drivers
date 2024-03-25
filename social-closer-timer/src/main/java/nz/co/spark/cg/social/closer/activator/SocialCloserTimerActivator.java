/*
 * SocialCloserTimerActivator.java
 */
package nz.co.spark.cg.social.closer.activator;

import nz.co.spark.cg.social.closer.task.SocialCloserTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver timer closer for SocialMiner Extractor.
 * @author rod
 * @since 2018-10-05
 */
public class SocialCloserTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(SocialCloserTimerActivator.class);
    private static BundleContext context;
    private SocialCloserTimer socialCloserTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        SocialCloserTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, socialCloserTimer = new SocialCloserTimer(), null);
        log.info("Social Closer Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        SocialCloserTimerActivator.context = null;
        socialCloserTimer.removeJob();
        log.info("Social Closer Timer has stopped successfully.");
    }
}
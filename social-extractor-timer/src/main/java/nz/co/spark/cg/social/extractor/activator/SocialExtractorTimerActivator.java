/*
 * SocialExtractorTimerActivator.java
 */
package nz.co.spark.cg.social.extractor.activator;

import nz.co.spark.cg.social.extractor.task.SocialExtractorTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver activator for Social Extractor Timer.
 * @author rod
 * @since 2018-10-04
 */
public class SocialExtractorTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(SocialExtractorTimerActivator.class);
    private static BundleContext context;
    private SocialExtractorTimer socialExtractorTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        SocialExtractorTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, socialExtractorTimer = new SocialExtractorTimer(), null);
        log.info("Social Extractor Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        SocialExtractorTimerActivator.context = null;
        socialExtractorTimer.removeJob();
        log.info("Social Extractor Timer has stopped successfully.");
    }
}
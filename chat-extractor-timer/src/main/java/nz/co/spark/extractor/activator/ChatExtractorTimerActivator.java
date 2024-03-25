/*
 * ChatExtractorTimerActivator.java
 */
package nz.co.spark.extractor.activator;

import nz.co.spark.extractor.task.ChatExtractorTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverInterface;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver activator for Extractor Timer.
 * @author rod
 * @since 2018-08-30
 */
public class ChatExtractorTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(ChatExtractorTimerActivator.class);
    private static BundleContext context;
    private ChatExtractorTimer chatExtractorTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        ChatExtractorTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName(), DriverInterface.class.getName()}, chatExtractorTimer = new ChatExtractorTimer(), null);
        log.info("Chat Extractor Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        ChatExtractorTimerActivator.context = null;
        chatExtractorTimer.removeJob();
        log.info("Chat Extractor Timer has stopped successfully.");
    }
}
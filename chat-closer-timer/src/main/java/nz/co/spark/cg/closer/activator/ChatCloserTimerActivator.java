/*
 * ChatCloserTimerActivator.java
 */
package nz.co.spark.cg.closer.activator;

import nz.co.spark.cg.closer.task.ChatCloserTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver timer closer for ChatMiner Extractor.
 * @author rod
 * @since 2018-08-30
 */
public class ChatCloserTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(ChatCloserTimerActivator.class);
    private static BundleContext context;
    private ChatCloserTimer chatCloserTimer;


    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        ChatCloserTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, chatCloserTimer = new ChatCloserTimer(), null);
        log.info("Chat Closer Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        ChatCloserTimerActivator.context = null;
        chatCloserTimer.removeJob();
        log.info("Chat Closer Timer has stopped successfully.");
    }
}
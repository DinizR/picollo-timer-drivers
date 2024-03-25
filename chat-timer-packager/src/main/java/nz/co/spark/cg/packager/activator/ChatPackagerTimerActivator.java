/*
 * ChatPackagerTimerActivator.java
 */
package nz.co.spark.cg.packager.activator;

import nz.co.spark.cg.packager.task.ChatPackagerTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.config.api.ConfigurationException;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver timer packager for ChatMiner Extractor.
 * @author rod
 * @since 2018-09-19
 */
public class ChatPackagerTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(ChatPackagerTimerActivator.class);
    private static BundleContext context;
    private static ChatPackagerTimer chatDBCleanerTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws ConfigurationException {
        ChatPackagerTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, chatDBCleanerTimer = new ChatPackagerTimer(), null);
        log.info("Chat Packager Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        ChatPackagerTimerActivator.context = null;
        log.info("Chat Packager Timer has stopped successfully.");
    }
}
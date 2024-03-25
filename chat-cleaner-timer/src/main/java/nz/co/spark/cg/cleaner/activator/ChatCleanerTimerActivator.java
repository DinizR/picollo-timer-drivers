/*
 * ChatCleanerTimerActivator.java
 */
package nz.co.spark.cg.cleaner.activator;

import nz.co.spark.cg.cleaner.task.ChatCleanerTimer;
import org.picollo.config.api.ConfigurableInterface;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver timer cleaner for Chat database cleaning.
 * @author rod
 * @since 2018-08-30
 */
public class ChatCleanerTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(ChatCleanerTimerActivator.class);
    private static BundleContext context;
    private ChatCleanerTimer chatCleanerTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws Exception {
        ChatCleanerTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, chatCleanerTimer = new ChatCleanerTimer(), null);
        log.info("Chat DB Cleaner Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        ChatCleanerTimerActivator.context = null;
        chatCleanerTimer.removeJob();
        log.info("Chat DB Cleaner Timer has stopped successfully.");
    }
}
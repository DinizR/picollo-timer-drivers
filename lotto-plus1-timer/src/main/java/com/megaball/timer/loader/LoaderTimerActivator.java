/*
 * LoaderTimerActivator.java
 */
package com.megaball.timer.loader;

import com.megaball.timer.loader.task.LoaderTimer;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.picollo.config.api.ConfigurableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loader driver.
 * @author rod
 * @since 2021-08
 */
public class LoaderTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(LoaderTimerActivator.class);
    private static BundleContext context;
    private LoaderTimer loaderTimer;

    private static final String MODULE_NAME = "loader.game.name";

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws org.picollo.config.api.ConfigurationException {
        LoaderTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, loaderTimer = new LoaderTimer(), null);
        log.info("Loader Timer for {} has started successfully.",loaderTimer.getConfigValue(MODULE_NAME));
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        LoaderTimerActivator.context = null;
        loaderTimer.removeJob();
        log.info("Loader Timer for {} has stopped successfully.",loaderTimer.getConfigValue(MODULE_NAME));
    }
}
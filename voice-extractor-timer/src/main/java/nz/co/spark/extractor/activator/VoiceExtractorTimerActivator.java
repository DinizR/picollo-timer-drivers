/*
 * VoiceExtractorTimerActivator.java
 */
package nz.co.spark.extractor.activator;

import nz.co.spark.extractor.task.VoiceExtractorTimer;
import org.cobra.config.api.ConfigurableInterface;
import org.cobra.timer.api.TimerSupplierInterface;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi driver activator for Voice Extractor Timer.
 * @author rod
 * @since 2018-12-04
 */
public class VoiceExtractorTimerActivator implements BundleActivator {
    private static final Logger log = LoggerFactory.getLogger(VoiceExtractorTimerActivator.class);
    private static BundleContext context;
    private VoiceExtractorTimer voiceExtractorTimer;

    public static BundleContext getContext() {
        return context;
    }

    public void start(BundleContext bundleContext) throws Exception {
        VoiceExtractorTimerActivator.context = bundleContext;
        context.registerService(new String[]{TimerSupplierInterface.class.getName(), ConfigurableInterface.class.getName()}, voiceExtractorTimer = new VoiceExtractorTimer(), null);
        log.info("Voice Extractor Timer has started successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    public void stop(BundleContext bundleContext) {
        VoiceExtractorTimerActivator.context = null;
        voiceExtractorTimer.removeJob();
        log.info("Voice Extractor Timer has stopped successfully.");
    }
}
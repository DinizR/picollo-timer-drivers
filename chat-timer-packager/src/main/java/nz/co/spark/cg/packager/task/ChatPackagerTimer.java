/*
* ChatPackagerTimer.java
 */
package nz.co.spark.cg.packager.task;

import org.cobra.config.api.ConfigurationException;
import org.cobra.driver.DriverType;
import org.cobra.timer.api.TimerSupplierInterface;
import org.cobra.timer.model.TriggerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Properties;

/**
* Module to extract wrap up XML files in a zip file and then encrypt it.
* @author rod
* @since 2018-09-19
 */
public class ChatPackagerTimer extends TimerSupplierInterface {
    private static final Logger log = LoggerFactory.getLogger(ChatPackagerTimer.class);
    private static final String EXECUTION_TIMER_CRON = "execution.timer.cron";
    private static final String extConfigDir = System.getProperty("ext.properties.dir") == null ? "" : System.getProperty("ext.properties.dir");
    protected static final Properties chatPackagerProperties = new Properties();
    private FileTime fileDate;

    public ChatPackagerTimer() throws ConfigurationException {
    }

    public TriggerType getTriggerType() {
        return TriggerType.CRON_EXPRESSION_TRIGGER;
    }

    @Override
    public String cronExpression() {
        return chatPackagerProperties.getProperty(EXECUTION_TIMER_CRON);
    }

    @Override
    public Class getJobClass() {
        return ChatPackagerJob.class;
    }

    @Override
    public boolean isConfigEnabled() {
        return true;
    }

    @Override
    public void loadConfig() throws ConfigurationException {
        URL url = null;
        boolean reload = false;

        try {
            url = new URL(extConfigDir+"/chat-packager.properties");
            FileTime fileTime = Files.getLastModifiedTime(Paths.get(url.toURI()));

            if( fileDate == null ) {
                fileDate = fileTime;
            } else if( fileDate.toInstant().equals(fileTime.toInstant()) ){
                return;
            } else {
                fileDate = fileTime;
                reload = true;
            }
            InputStream input = new FileInputStream(url.getPath());
            chatPackagerProperties.load(input);
            input.close();

            if( reload ) {
                setupTrigger();
            }
        } catch (MalformedURLException e) {
            log.error("Chat Packager reading properties error. Parsing url: {}",url);
            throw new ConfigurationException(e.getMessage());
        } catch (FileNotFoundException e) {
            log.error("Chat Packager reading properties error. File not found: {}",url);
            throw new ConfigurationException(e.getMessage());
        } catch (IOException | URISyntaxException e) {
            log.error("Chat Packager reading properties error. I/O error: {}",e.getMessage());
            throw new ConfigurationException(e.getMessage());
        }
    }

    @Override
    public DriverType getType() {
        return DriverType.Custom;
    }

    @Override
    public String getName() {
        return "Chat Packager Timer Plugin.";
    }

    @Override
    public String getDescription() {
        return "This timer plugin intents to create a zip from chat extractions captured from UCS database.";
    }
}
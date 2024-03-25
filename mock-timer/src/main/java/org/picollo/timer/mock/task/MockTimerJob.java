/*
* MockTimerJob.java
 */
package org.picollo.timer.mock.task;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.util.stream.IntStream;

/**
 * Mock Job.
 * @author rod
 * @since 2019-04
*/
@DisallowConcurrentExecution
public class MockTimerJob implements Job {

    public MockTimerJob() {
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext){
        IntStream.range(0, 15).forEach(i -> {
            MockTimer.logger.info("Next number is : {}",i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        });
    }
}

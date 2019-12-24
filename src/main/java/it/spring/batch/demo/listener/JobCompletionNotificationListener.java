package it.spring.batch.demo.listener;

import org.joda.time.DateTime;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private DateTime startTime, stopTime;

	
	@Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = new DateTime();
    }
	
    @Override
    public void afterJob(JobExecution jobExecution) {
    	stopTime = new DateTime();
        System.out.println("Batch Job stops at: " + stopTime.getSecondOfMinute() + " seconds");
        System.out.println("Total time take in seconds: " + getTimeInSeconds(startTime , stopTime));
        
    	if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("JOB FINISHED! Time to check results!");
        } 
    }
    
    private long getTimeInSeconds(DateTime start, DateTime stop){
        return stop.getSecondOfMinute() - start.getSecondOfMinute();
    }
}
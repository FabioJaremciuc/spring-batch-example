package it.spring.batch.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.boot.autoconfigure.batch.JobExecutionExitCodeGenerator;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    JobExecutionExitCodeGenerator jobExecutionExitCodeGenerator;

    @Autowired
    Job job;
	
    public static void main(String[] args) {
        int exitCode = SpringApplication.exit(
                SpringApplication.run(DemoApplication.class, args));

        System.exit(exitCode);

    }

    @Override
    public void run(String... args) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("JobID", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();

        jobExecutionExitCodeGenerator.onApplicationEvent(new JobExecutionEvent(jobLauncher.run(job, params)));
    }


}

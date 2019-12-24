package it.spring.batch.demo.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import it.spring.batch.demo.listener.JobCompletionNotificationListener;
import it.spring.batch.demo.model.Address;
import it.spring.batch.demo.model.Cities;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private StepBuilderFactory steps;

    @Value("${batch.output.address.path}")
    public String outputPathAddress;

    @Value("${batch.output.cities.path}")
    public String outputPathCities;

    @Value("${batch.output.address.filename}")
    public String outputAddressFileName;

    @Value("${batch.output.cities.filename}")
    public String outputCitiesFileName;

    @Value("${batch.output.filetype}")
    public String outputFileType;
    
    private static final String QUERY_FIND_ALL_ADDRESS = "SELECT * FROM sakila.address;";

    private static final String QUERY_FIND_ALL_CITIES = "SELECT * FROM sakila.city;";
	
    @Bean("dataSource")
    @ConfigurationProperties(prefix = "app.datasource")
    public DataSource dataSource() {
    	return DataSourceBuilder
                .create()
                .build();
    }
    
	public ItemReader<Address> readerAllAddress() {
		JdbcCursorItemReader<Address> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource());
		reader.setSql(QUERY_FIND_ALL_ADDRESS);
		reader.setRowMapper(new BeanPropertyRowMapper<>(Address.class));
		return reader;
	}

	public ItemReader<Cities> readerAllCities() {
		JdbcCursorItemReader<Cities> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource());
		reader.setSql(QUERY_FIND_ALL_CITIES);
		reader.setRowMapper(new BeanPropertyRowMapper<>(Cities.class));
		return reader;
	}

	public FlatFileItemWriter<Address> writerAllAddress(FileSystemResource resource) {
		BeanWrapperFieldExtractor<Address> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<Address>();
		beanWrapperFieldExtractor.setNames(new String[] { 
				"address_id", 
				"address",
				"district",
				"city_id",
				"postal_code",
				"last_update"
				});
		DelimitedLineAggregator<Address> delimitedLineAggregator = new DelimitedLineAggregator<Address>();
		delimitedLineAggregator.setDelimiter(";");
		delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);

		return new FlatFileItemWriterBuilder<Address>()
					.name("TXTWriter")
					.resource(resource)
					.append(false)
					.lineAggregator(delimitedLineAggregator)
					.build();
	}

	public FlatFileItemWriter<Cities> writerAllCities(FileSystemResource resource) {
		BeanWrapperFieldExtractor<Cities> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<Cities>();
		beanWrapperFieldExtractor.setNames(new String[] { 
				"city_id",
				"city",
				"country_id",
				"last_update",
		});
		DelimitedLineAggregator<Cities> delimitedLineAggregator = new DelimitedLineAggregator<Cities>();
		delimitedLineAggregator.setDelimiter(";");
		delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);
		
		return new FlatFileItemWriterBuilder<Cities>()
				.name("TXTWriter")
				.resource(resource)
				.append(false)
				.lineAggregator(delimitedLineAggregator)
				.build();
	}

	@Bean
	public Step stepAddress() {
		return steps.get("stepAddress").<Address, Address>chunk(10)
				.reader(readerAllAddress())
				.writer(writerAllAddress(
							new FileSystemResource(outputPathAddress
		                    .concat(File.separator)
		                    .concat(outputAddressFileName)
		                    .concat(getFormattedTimeNow())
		                    .concat(outputFileType))))
				.build();
	}

	@Bean
	public Step stepCities() {
		return steps.get("stepCities").<Cities, Cities>chunk(10)
				.reader(readerAllCities())
				.writer(writerAllCities(
						new FileSystemResource(outputPathCities
							.concat(File.separator)
							.concat(outputCitiesFileName)
							.concat(getFormattedTimeNow())
							.concat(outputFileType))))
				.build();
	}

	@Bean
	public Job demoJob(JobBuilderFactory jobs, JobCompletionNotificationListener listener) throws IOException {
		folderCheck(outputPathAddress);
		folderCheck(outputPathCities);
		
		List<Step> steps = new ArrayList<>();
		   steps.add(stepAddress());
		   steps.add(stepCities());
		
		return jobs.get("demoJob").incrementer(new RunIdIncrementer()).listener(listener)
				.start(createParallelFlow(steps))
				.end()
				.build();
	}

	private static Flow createParallelFlow(List<Step> steps) {
	    SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
	    taskExecutor.setConcurrencyLimit(steps.size());

	    List<Flow> flows = steps.stream() 
	        .map(step -> 
	                new FlowBuilder<Flow>("flow_" + step.getName())
	                .start(step) 
	                .build()) 
	            .collect(Collectors.toList());

	    return new FlowBuilder<SimpleFlow>("parallelStepsFlow").split(taskExecutor) 
	        .add(flows.toArray(new Flow[flows.size()])) 
	        .build();
	}
	
	private void folderCheck(String folderPath) throws IOException {
		File f = new File(folderPath); 
		
		if(!f.exists() && !f.isDirectory()) {
			Files.createDirectory(Paths.get(folderPath));
		}
	}
	
	public String getFormattedTimeNow() {
		SimpleDateFormat formatter= new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
		return formatter.format(new Date(System.currentTimeMillis()));
	}

}
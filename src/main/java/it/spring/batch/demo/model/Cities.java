package it.spring.batch.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Cities {

	private Integer city_id; 
	private String city; 
	private Integer country_id; 
	private String last_update; 
	
	
}

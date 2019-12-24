package it.spring.batch.demo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Address {

    private Integer address_id;
    private String address;
    private String address_2;
    private String district;
    private Integer city_id;
    private Integer postal_code;
    private String location;
    private String last_update;

    
}
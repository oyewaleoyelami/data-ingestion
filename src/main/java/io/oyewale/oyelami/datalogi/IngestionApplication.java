package io.oyewale.oyelami.datalogi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IngestionApplication {

	public static void main(String[] args) {
        SpringApplication.run(IngestionApplication.class, args);
	}

}

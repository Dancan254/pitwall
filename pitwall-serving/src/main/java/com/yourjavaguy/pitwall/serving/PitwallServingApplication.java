package com.yourjavaguy.pitwall.serving;

import com.yourjavaguy.pitwall.serving.config.ServingProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties(ServingProperties.class)
@EnableScheduling
public class PitwallServingApplication {

    public static void main(String[] args) {
        SpringApplication.run(PitwallServingApplication.class, args);
    }
}

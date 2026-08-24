package com.yourjavaguy.pitwall.source;

import com.yourjavaguy.pitwall.source.config.SourceProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties(SourceProperties.class)
@EnableScheduling
public class PitwallSourceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PitwallSourceApplication.class, args);
    }
}

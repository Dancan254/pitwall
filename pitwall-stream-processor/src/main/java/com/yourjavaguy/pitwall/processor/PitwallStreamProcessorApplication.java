package com.yourjavaguy.pitwall.processor;

import com.yourjavaguy.pitwall.processor.config.ProcessorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties(ProcessorProperties.class)
@EnableScheduling
public class PitwallStreamProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(PitwallStreamProcessorApplication.class, args);
    }
}

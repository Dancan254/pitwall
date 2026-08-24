package com.yourjavaguy.pitwall.commons.autoconfigure;

import com.yourjavaguy.pitwall.commons.exception.GlobalExceptionHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ProblemDetail;

@AutoConfiguration
@ConditionalOnWebApplication
@ConditionalOnClass(ProblemDetail.class)
public class WebExceptionAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    GlobalExceptionHandler pitwallGlobalExceptionHandler() {
        return new GlobalExceptionHandler();
    }

}

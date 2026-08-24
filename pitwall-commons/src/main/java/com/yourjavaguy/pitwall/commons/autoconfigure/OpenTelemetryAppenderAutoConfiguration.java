package com.yourjavaguy.pitwall.commons.autoconfigure;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

// Ordered by name so commons keeps no compile dependency on the OTel starter.
// Without the ordering, @ConditionalOnBean runs before the SDK auto-configuration
// has registered OpenTelemetry, the installer is skipped, and the appender drops
// every record in silence.
@AutoConfiguration
@AutoConfigureAfter(name = "org.springframework.boot.opentelemetry.autoconfigure.OpenTelemetrySdkAutoConfiguration")
@ConditionalOnClass({OpenTelemetryAppender.class, OpenTelemetry.class})
public class OpenTelemetryAppenderAutoConfiguration {

    @Bean
    @ConditionalOnBean(OpenTelemetry.class)
    InitializingBean openTelemetryAppenderInstaller(OpenTelemetry openTelemetry) {
        return () -> OpenTelemetryAppender.install(openTelemetry);
    }
}

package com.yourjavaguy.pitwall.commons.exception;

import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class StreamingDisconnectTest {

    private static Method disconnectHandler() {
        return Arrays.stream(GlobalExceptionHandler.class.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(ExceptionHandler.class))
                .filter(method -> Arrays.asList(method.getAnnotation(ExceptionHandler.class).value())
                        .contains(AsyncRequestNotUsableException.class))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no handler registered for a client disconnect"));
    }

    @Test
    void should_register_a_dedicated_handler_when_a_streaming_client_disconnects() {
        assertThat(disconnectHandler().getName()).isEqualTo("handleClientDisconnect");
    }

    @Test
    void should_write_no_response_body_when_a_streaming_client_disconnects() {
        assertThat(disconnectHandler().getReturnType()).isEqualTo(void.class);
    }
}

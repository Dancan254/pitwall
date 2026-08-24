package com.yourjavaguy.pitwall.commons.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void should_return_404_problem_detail_when_resource_not_found() {
        var problemDetail = handler.handleNotFound(new ResourceNotFoundException("Car", 42));

        assertThat(problemDetail.getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(problemDetail.getTitle()).isEqualTo("Resource Not Found");
        assertThat(problemDetail.getDetail()).isEqualTo("Car not found: 42");
    }

    @Test
    void should_return_500_problem_detail_when_unexpected_error() {
        var problemDetail = handler.handleGeneric(new IllegalStateException("boom"));

        assertThat(problemDetail.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(problemDetail.getTitle()).isEqualTo("Internal Server Error");
        assertThat(problemDetail.getDetail()).isEqualTo("boom");
    }
}

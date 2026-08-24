package com.yourjavaguy.pitwall.source.web.dto;

import jakarta.validation.constraints.NotBlank;

public record AnomalyRequest(
        @NotBlank String carId,
        @NotBlank String sensorId
) {
}

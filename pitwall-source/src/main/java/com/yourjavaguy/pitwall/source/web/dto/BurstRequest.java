package com.yourjavaguy.pitwall.source.web.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record BurstRequest(
        @NotNull @DecimalMin("1.0") @DecimalMax("50.0") Double multiplier,
        @NotNull @Min(1) @Max(600) Integer durationSeconds
) {
}

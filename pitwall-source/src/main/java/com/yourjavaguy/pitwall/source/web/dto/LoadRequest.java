package com.yourjavaguy.pitwall.source.web.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record LoadRequest(
        @NotNull @Min(1) @Max(200) Integer cars,
        @NotNull @Min(1) @Max(2000) Integer sensorsPerCar,
        @NotNull @DecimalMin("0.01") @DecimalMax("100.0") Double rateScale
) {
}

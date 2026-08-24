package com.yourjavaguy.pitwall.source.web.dto;

import jakarta.validation.constraints.NotNull;

public record DropoutRequest(@NotNull Boolean active) {
}

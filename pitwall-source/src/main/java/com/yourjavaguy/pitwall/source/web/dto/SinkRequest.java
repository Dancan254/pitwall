package com.yourjavaguy.pitwall.source.web.dto;

import jakarta.validation.constraints.NotBlank;

public record SinkRequest(@NotBlank String name) {
}

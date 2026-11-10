package com.app.consumer;

import java.util.UUID;

public record TaskMessage(UUID id, String payload, long sentAt) {
}

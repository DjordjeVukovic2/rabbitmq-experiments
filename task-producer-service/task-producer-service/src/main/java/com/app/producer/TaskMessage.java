package com.app.producer;

import java.util.UUID;

public record TaskMessage(UUID id, String payload, long sentAt) {

}

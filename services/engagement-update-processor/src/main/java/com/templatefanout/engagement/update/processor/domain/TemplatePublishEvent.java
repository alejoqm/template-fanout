package com.templatefanout.engagement.update.processor.domain;

import java.time.Instant;
import java.util.List;

public record TemplatePublishEvent(String eventId, String templateId, String version,
                                   List<String> affectedFirms) {

    public TemplatePublishEvent {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required for idempotency");
        }
        if (templateId == null || templateId.isBlank()) {
            throw new IllegalArgumentException("templateId is required");
        }
        if (version == null || version.isBlank()) {
            throw new IllegalArgumentException("version is required");
        }
    }
}

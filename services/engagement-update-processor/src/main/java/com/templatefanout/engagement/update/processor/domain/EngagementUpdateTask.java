package com.templatefanout.engagement.update.processor.domain;

public record EngagementUpdateTask(Long taskId,
                                   String engagementId,
                                   String firmId,
                                   String templateId,
                                   String templateVersion) {
}

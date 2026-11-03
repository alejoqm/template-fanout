package com.templatefanout.engagement.update.processor.ports.output;

import com.templatefanout.engagement.update.processor.domain.EngagementUpdateTask;
import com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent;
import com.templatefanout.engagement.update.processor.domain.UpdateStatus;

import java.util.List;

public interface EngagementAffectedStorage {

    boolean isAlreadyProcessed(String eventId);

    List<EngagementUpdateTask> readTasks(List<String> firmIds, String templateId);

    boolean recordEventIfAbsent(TemplatePublishEvent event);

    void updateStatus(Long taskId, UpdateStatus status, String errorMessage);
}

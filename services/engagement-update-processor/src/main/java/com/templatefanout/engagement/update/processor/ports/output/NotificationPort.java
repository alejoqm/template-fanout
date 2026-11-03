package com.templatefanout.engagement.update.processor.ports.output;

public interface NotificationPort {

    void notifySummaryReady(String engagementId, String summaryId);
}

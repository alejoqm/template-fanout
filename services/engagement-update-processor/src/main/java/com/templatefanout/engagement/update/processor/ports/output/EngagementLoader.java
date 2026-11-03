package com.templatefanout.engagement.update.processor.ports.output;

import com.templatefanout.engagement.update.processor.domain.EngagementSnapshot;

public interface EngagementLoader {

    EngagementSnapshot loadEngagement(String engagementId);

    void applyTemplateDelta(String engagementId, int targetVersion);
}

package com.templatefanout.engagement.update.processor.ports.input;

import com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent;

public interface EngagementUpdateUseCase {

    boolean handleTemplatePublishEvent(TemplatePublishEvent templatePublishEvent);

}

package com.templatefanout.engagement.update.processor.adapters.input;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.templatefanout.engagement.update.processor.application.EngagementUpdateService;
import com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Kafka adapter for consuming product template publish events.
 *
 * Event flow:
 *  1. Receive TEMPLATE_PUBLISHED event from Kafka
 *  2. Delegate to application layer (EngagementUpdateService) for business logic
 *  3. Service validates idempotency via storage port before processing
 *
 * Design decisions:
 *  - Uses manual acknowledgment (Acknowledgment parameter) so we can ack only
 *    after the event is successfully recorded AND processing has started.
 *    This prevents event loss if the service crashes mid-processing.
 *  - @KafkaListener with groupId configured in application.properties ensures
 *    each regional processor participates in consumer group load balancing.
 *  - No retry decorator here — retry policy lives in the service layer where
 *    we have visibility into whether the event was already processed.
 */
@Component
public class HandleTemplatePublishEvent {

    private static final Logger log = LoggerFactory.getLogger(HandleTemplatePublishEvent.class);

    private final EngagementUpdateService updateService;

    public HandleTemplatePublishEvent(EngagementUpdateService updateService) {
        this.updateService = updateService;
    }

    /**
     * Consume TEMPLATE_PUBLISHED events from Kafka.
     *
     * Topic: template-publishes (configured in application.properties)
     * Group: engagement-update-processor-{region} (each region gets its own group)
     *
     * @param event      JSON-serialized TemplatePublishEvent payload
     * @param ack        Manual acknowledgment for precise commit timing
     */
    @KafkaListener(
            topics = "${spring.kafka.template.default-topic}",
            containerFactory = "templatePublishContainerFactory"
    )
    public void onTemplatePublished(
            TemplatePublishEvent event,
            Acknowledgment ack) {

        try {
            log.info("Received template publish event: templateId={}, version={}, eventId={}",
                    event.templateId(), event.version(), event.eventId());

            // Delegate to service layer for idempotency check and processing kick-off
            boolean isNew = updateService.handleTemplatePublishEvent(event);

            if (isNew) {
                log.info("Started async processing for template {} version {}",
                        event.templateId(), event.version());
            } else {
                log.debug("Event already processed, skipping: eventId={}", event.eventId());
            }

            // Commit offset only after event is safely recorded and processing started
            ack.acknowledge();
            log.debug("Committed offset for eventId={}", event.eventId());

        } catch (IllegalArgumentException e) {
            // Malformed event — poison pill. Log and ack to prevent infinite retry loop.
            log.error("Malformed template publish event, discarding: eventId={}",
                    event.eventId(), e);
            ack.acknowledge();

        } catch (Exception e) {
            // Service layer threw unexpected error. Re-throw to trigger Kafka retry
            // (configurable via container factory's retry policy).
            log.error("Failed to process template publish event: eventId={}",
                    event.eventId(), e);
            throw e;
        }
    }
}

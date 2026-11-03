package com.templatefanout.engagement.update.processor.application;

import com.templatefanout.engagement.update.processor.domain.EngagementUpdateTask;
import com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent;
import com.templatefanout.engagement.update.processor.ports.output.EngagementAffectedStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link EngagementUpdateService}.
 *
 * Focus: core business logic, not integration details.
 * Uses mocked storage to avoid database/Kafka dependencies.
 */
@ExtendWith(MockitoExtension.class)
class EngagementUpdateServiceTest {

    @Mock
    private EngagementAffectedStorage storage;

    @Mock
    private Future<?> mockFuture;

    private EngagementUpdateService service;

    @Captor
    private ArgumentCaptor<TemplatePublishEvent> eventCaptor;

    @BeforeEach
    void setUp() {
        // Create service with mocked storage
        // Note: max-concurrency=100 for tests, load-timeout=60s
        service = new EngagementUpdateService(storage, 100, 60);
    }

    @Test
    @DisplayName("Handles new template publish event correctly")
    void handleTemplatePublishEvent_newEvent_returnsTrue() {
        // Given
        TemplatePublishEvent event = new TemplatePublishEvent(
            "evt-001", "TPL-AUDIT-2024", "v5.2", List.of("FIRM-001", "FIRM-002")
        );

        when(storage.isAlreadyProcessed(event.eventId())).thenReturn(false);
        when(storage.recordEventIfAbsent(any(TemplatePublishEvent.class))).thenReturn(true);
        when(storage.readTasks(eq(event.affectedFirms()), eq(event.templateId())))
            .thenReturn(Collections.emptyList());

        // When
        boolean result = service.handleTemplatePublishEvent(event);

        // Then
        assertThat(result).isTrue();

        verify(storage, times(1)).isAlreadyProcessed(event.eventId());
        verify(storage, times(1)).recordEventIfAbsent(eventCaptor.capture());
        verify(storage, times(1)).readTasks(event.affectedFirms(), event.templateId());
        verify(storage, never()).updateStatus(any(), any(), any());
    }

    @Test
    @DisplayName("Suppresses duplicate events")
    void handleTemplatePublishEvent_alreadyProcessed_returnsFalse() {
        // Given
        TemplatePublishEvent event = new TemplatePublishEvent(
            "evt-001", "TPL-AUDIT-2024", "v5.2", List.of("FIRM-001")
        );

        when(storage.isAlreadyProcessed(event.eventId())).thenReturn(true);

        // When
        boolean result = service.handleTemplatePublishEvent(event);

        // Then
        assertThat(result).isFalse();

        verify(storage, never()).recordEventIfAbsent(any());
        verify(storage, never()).readTasks(any(), any());
    }

    @Test
    @DisplayName("Handles race condition when another consumer records event first")
    void handleTemplatePublishEvent_raceCondition_returnsFalse() {
        // Given
        TemplatePublishEvent event = new TemplatePublishEvent(
            "evt-001", "TPL-AUDIT-2024", "v5.2", List.of("FIRM-001")
        );

        when(storage.isAlreadyProcessed(event.eventId())).thenReturn(false);
        when(storage.recordEventIfAbsent(any(TemplatePublishEvent.class))).thenReturn(false);

        // When
        boolean result = service.handleTemplatePublishEvent(event);

        // Then
        assertThat(result).isFalse();

        verify(storage, times(1)).isAlreadyProcessed(event.eventId());
        verify(storage, times(1)).recordEventIfAbsent(any());
        verify(storage, never()).readTasks(any(), any());
    }

    @Test
    @DisplayName("Does not dispatch tasks when none are found")
    void startFanOut_noTasks_logsInfo() {
        // Given
        TemplatePublishEvent event = new TemplatePublishEvent(
            "evt-001", "TPL-AUDIT-2024", "v5.2", List.of("FIRM-001")
        );

        when(storage.recordEventIfAbsent(any())).thenReturn(true);
        when(storage.readTasks(event.affectedFirms(), event.templateId()))
            .thenReturn(Collections.emptyList());

        // When
        boolean result = service.handleTemplatePublishEvent(event);

        // Then
        assertThat(result).isTrue();
        verify(storage, times(1)).readTasks(event.affectedFirms(), event.templateId());
    }

    @Test
    @DisplayName("Dispatches tasks when pending engagements exist")
    void startFanOut_hasTasks_submitsToExecutor() throws InterruptedException {
        // Given
        TemplatePublishEvent event = new TemplatePublishEvent(
            "evt-001", "TPL-AUDIT-2024", "v5.2", List.of("FIRM-001", "FIRM-002")
        );

        List<EngagementUpdateTask> tasks = List.of(
            new EngagementUpdateTask(1L, "ENG-001", "FIRM-001", "TPL-AUDIT-2024", "v5.2"),
            new EngagementUpdateTask(2L, "ENG-002", "FIRM-001", "TPL-AUDIT-2024", "v5.2"),
            new EngagementUpdateTask(3L, "ENG-003", "FIRM-002", "TPL-AUDIT-2024", "v5.2")
        );

        when(storage.recordEventIfAbsent(any())).thenReturn(true);
        when(storage.readTasks(event.affectedFirms(), event.templateId())).thenReturn(tasks);

        // When
        boolean result = service.handleTemplatePublishEvent(event);

        // Then
        assertThat(result).isTrue();
        verify(storage, times(1)).readTasks(event.affectedFirms(), event.templateId());

        // Wait briefly for async submission
        // This sleep is too long to validate Completed is called.
        Thread.sleep(3100);

        // 3 tasks should have been processed (each updates status to PROCESSING then COMPLETED)
        verify(storage, times(3)).updateStatus(any(Long.class), eq(com.templatefanout.engagement.update.processor.domain.UpdateStatus.PROCESSING), eq(null));
        verify(storage, times(3)).updateStatus(any(Long.class), eq(com.templatefanout.engagement.update.processor.domain.UpdateStatus.COMPLETED), eq(null));
    }

    @Test
    @DisplayName("Records the event that was passed in")
    void handleTemplatePublishEvent_recordsCorrectEvent() {
        // Given
        TemplatePublishEvent event = new TemplatePublishEvent(
            "evt-001", "TPL-AUDIT-2024", "v5.2", List.of("FIRM-001", "FIRM-002")
        );

        when(storage.isAlreadyProcessed(event.eventId())).thenReturn(false);
        when(storage.recordEventIfAbsent(any(TemplatePublishEvent.class))).thenReturn(true);
        when(storage.readTasks(any(), any())).thenReturn(Collections.emptyList());

        // When
        service.handleTemplatePublishEvent(event);

        // Then
        verify(storage).recordEventIfAbsent(eventCaptor.capture());
        TemplatePublishEvent recordedEvent = eventCaptor.getValue();

        assertThat(recordedEvent.eventId()).isEqualTo("evt-001");
        assertThat(recordedEvent.templateId()).isEqualTo("TPL-AUDIT-2024");
        assertThat(recordedEvent.version()).isEqualTo("v5.2");
        assertThat(recordedEvent.affectedFirms()).containsExactlyInAnyOrder("FIRM-001", "FIRM-002");
    }
}

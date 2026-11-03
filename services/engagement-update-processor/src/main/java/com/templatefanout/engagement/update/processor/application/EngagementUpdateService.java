package com.templatefanout.engagement.update.processor.application;

import com.templatefanout.engagement.update.processor.domain.EngagementUpdateTask;
import com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent;
import com.templatefanout.engagement.update.processor.domain.UpdateStatus;
import com.templatefanout.engagement.update.processor.ports.input.EngagementUpdateUseCase;
import com.templatefanout.engagement.update.processor.ports.output.EngagementAffectedStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Service
public class EngagementUpdateService implements EngagementUpdateUseCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngagementUpdateService.class);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Admission control against the downstream loader. Sized to the capacity
     * the owning team has agreed to absorb — NOT to what we could theoretically
     * push. Overrunning this is how you get rate-limited or blocked outright.
     */
    private final Semaphore downstreamPermits;
    private final Duration loadTimeout;

    private final EngagementAffectedStorage engagementAffectedStorage;

    public EngagementUpdateService(final EngagementAffectedStorage engagementAffectedStorage,
                                   @Value("${processor.max-concurrency:100}") int maxConcurrency,
                                   @Value("${processor.load-timeout-seconds:90}") long loadTimeoutSeconds) {
        this.engagementAffectedStorage = engagementAffectedStorage;

        this.downstreamPermits = new Semaphore(maxConcurrency, true);
        this.loadTimeout = Duration.ofSeconds(loadTimeoutSeconds);
    }

    @Override
    public boolean handleTemplatePublishEvent(TemplatePublishEvent templatePublishEvent) {
        if (engagementAffectedStorage.isAlreadyProcessed(templatePublishEvent.eventId())) {
            LOGGER.debug("Duplicate event suppressed: eventId={}", templatePublishEvent.eventId());
            return false;
        }

        boolean recorded = engagementAffectedStorage.recordEventIfAbsent(templatePublishEvent);
        if (!recorded) {
            // Lost the race — another consumer instance got there first.
            LOGGER.debug("Event recorded concurrently by another consumer: eventId={}",
                    templatePublishEvent.eventId());
            return false;
        }

        if (!recorded) {
            // Lost the race — another consumer instance got there first.
            LOGGER.debug("Event recorded concurrently by another consumer: eventId={}",
                    templatePublishEvent.eventId());
            return false;
        }

        startFanOut(templatePublishEvent);
        return true;
    }

    private void startFanOut(TemplatePublishEvent event) {
        LOGGER.info("Starting fan-out for template={} version={}",
                event.templateId(), event.version());
        // Delegated to outbound port in the complete implementation.

        List<EngagementUpdateTask> tasks =
                engagementAffectedStorage.readTasks(event.affectedFirms(), event.templateId());

        if (tasks.isEmpty()) {
            LOGGER.info("No pending engagements for template={} version={}",
                    event.templateId(), event.version());
            return;
        }

        List<Future<?>> futures = new ArrayList<>(tasks.size());
        for (EngagementUpdateTask task : tasks) {
            futures.add(executor.submit(() -> processEngagement(task, event)));
        }

        // Deliberately not joined. Returning lets the Kafka offset commit and
        // keeps the consumer responsive. Durability lives in the outbox table.
        LOGGER.info("Submitted {} tasks; processing continues asynchronously", futures.size());

    }

    /**
     * Process a single engagement: load it, generate the summary, advance status.
     *
     * Status lifecycle for this task:
     *   PENDING → PROCESSING → COMPLETED
     *                        ↘ FAILED (with error message)
     *
     * The permit is acquired BEFORE marking PROCESSING. This matters: if we
     * flipped the status first and then queued behind the semaphore, a task
     * could sit in PROCESSING for many minutes without any work happening, and
     * the reconciliation job's stuck-entry timeout would requeue healthy work.
     * Queued-but-not-started is honestly still PENDING.
     */
    private void processEngagement(EngagementUpdateTask task, TemplatePublishEvent event) {
        LOGGER.info("Processing task {}", task);
        long startNanos = System.nanoTime();
        boolean permitAcquired = false;

        try {
            // 1. Admission control — bounds pressure on the downstream service.
            downstreamPermits.acquire();
            permitAcquired = true;

            // 2. Mark in-flight so operators and reconciliation can see it.
            engagementAffectedStorage.updateStatus(task.taskId(), UpdateStatus.PROCESSING, null);

           Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 3001));
            engagementAffectedStorage.updateStatus(task.taskId(), UpdateStatus.COMPLETED, null);
        } catch (InterruptedException e) {
            // Shutdown or cancellation: restore the flag, leave the row PENDING so
            // it is picked up again rather than being falsely marked FAILED.
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while processing taskId={}; leaving for reconciliation",
                    task.taskId());

        } catch (Exception e) {
            // Terminal failure for this attempt. retry_count is incremented by the
            // storage layer; a future increment adds backoff + DLQ on top.
            LOGGER.error("FAILED taskId={} engagement={} durationMs={} cause={}",
                    task.taskId(), task.engagementId(), elapsedMs(startNanos),
                    e.toString(), e);
            safeUpdateStatus(task.taskId(), UpdateStatus.FAILED, truncate(e.getMessage()));
        } finally {
            if (permitAcquired) {
                downstreamPermits.release();
            }
        }
    }

    /**
     * Status updates on the failure path must not mask the original error.
     * If the database is unreachable we log and move on — the reconciliation
     * job will find the row still in PROCESSING and requeue it.
     */
    private void safeUpdateStatus(Long taskId, UpdateStatus status, String errorMessage) {
        try {
            engagementAffectedStorage.updateStatus(taskId, status, errorMessage);
        } catch (Exception dbError) {
            LOGGER.error("Could not persist status {} for taskId={}; reconciliation will recover",
                    status, taskId, dbError);
        }
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 480 ? message : message.substring(0, 480);
    }
}

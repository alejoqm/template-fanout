package com.templatefanout.engagement.update.processor.adapters.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.templatefanout.engagement.update.processor.domain.EngagementUpdateTask;
import com.templatefanout.engagement.update.processor.domain.TemplatePublishEvent;
import com.templatefanout.engagement.update.processor.domain.UpdateStatus;
import com.templatefanout.engagement.update.processor.ports.output.EngagementAffectedStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * PostgreSQL adapter for {@link EngagementAffectedStorage}.
 *
 * Implements three distinct responsibilities:
 *  1. Idempotency: {@code isAlreadyProcessed} checks the processed_template_events table.
 *  2. Work acquisition: {@code claimTasks} atomically claims a batch of PENDING rows from
 *     outbox_entries using FOR UPDATE SKIP LOCKED, so concurrent processor instances
 *     never hand out the same task twice.
 *  3. State mutation: {@code updateStatus} advances outbox rows.
 *
 * All queries use explicit SQL (no ORM) for maximum control over locking,
 * pagination, and constraint enforcement.
 */
@Repository
public class PostgresEngagementAffectedStorage implements EngagementAffectedStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresEngagementAffectedStorage.class);

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    /**
     * RowMapper for mapping outbox rows to EngagementUpdateTask records.
     */
    private static final RowMapper<EngagementUpdateTask> TASK_ROW_MAPPER = (rs, rowNum) ->
            new EngagementUpdateTask(
                    rs.getLong("task_id"),
                    rs.getString("engagement_id"),
                    rs.getString("firm_id"),
                    rs.getString("template_id"),
                    rs.getString("template_version")
            );

    public PostgresEngagementAffectedStorage(final NamedParameterJdbcTemplate jdbc,
                                             final ObjectMapper objectMapper,
                                             @Value("${processor.batch-size:1000}") final int batchSize) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
    }

    /**
     * Check if an event_id was already recorded (for idempotency).
     * Uses a simple SELECT with index on event_id.
     */
    @Override
    public boolean isAlreadyProcessed(String eventId) {
        String sql = """
            SELECT 1
              FROM processed_template_events
             WHERE event_id = :eventId
             LIMIT 1
            """;

        List<Integer> rows = jdbc.query(sql, new MapSqlParameterSource("eventId", eventId),
                (rs, rowNum) -> rs.getInt(1)
        );

        return !rows.isEmpty();
    }

    /**
     * Atomically claim a batch of PENDING tasks for the given firms and template.
     *
     * The inner SELECT locks the candidate rows (FOR UPDATE SKIP LOCKED) so that
     * competing processor instances simply skip rows another instance is already
     * claiming, then the outer UPDATE flips them to PROCESSING in the same
     * statement. A task can therefore only ever be handed out to one instance.
     *
     * Batch size comes from {@code processor.batch-size} so long fan-outs drain
     * in bounded chunks instead of one giant read.
     */
    @Override
    public List<EngagementUpdateTask> claimTasks(List<String> firmIds, String templateId) {
        String sql = """
            WITH claimed AS (
                SELECT task_id
                  FROM outbox_entries
                 WHERE template_id = :templateId
                   AND firm_id IN (:firmIds)
                   AND status = 'PENDING'
                 ORDER BY created_at ASC
                 LIMIT :batchSize
                 FOR UPDATE SKIP LOCKED
            )
            UPDATE outbox_entries o
               SET status = 'PROCESSING',
                   updated_at = NOW()
              FROM claimed c
             WHERE o.task_id = c.task_id
            RETURNING o.task_id, o.engagement_id, o.firm_id, o.template_id, o.template_version
            """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("templateId", templateId)
                .addValue("firmIds", firmIds)
                .addValue("batchSize", batchSize);

        List<EngagementUpdateTask> tasks = jdbc.query(sql, params, TASK_ROW_MAPPER);

        if (!tasks.isEmpty()) {
            LOGGER.debug("Claimed {} tasks for template={} firms={}",
                    tasks.size(), templateId, firmIds.size());
        }

        return tasks;
    }

    @Override
    public boolean recordEventIfAbsent(TemplatePublishEvent event) {
        String sql = """
            INSERT INTO processed_template_events
                (event_id, template_id, version, affected_firms, created_at)
            VALUES
                (:eventId, :templateId, :version, :affectedFirms::jsonb, NOW())
            ON CONFLICT (event_id) DO NOTHING
            """;


        String firmsJson;
        try {
            firmsJson = objectMapper.writeValueAsString(event.affectedFirms());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize affectedFirms for event " + event.eventId(), e);
        }


        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("eventId", event.eventId())
                .addValue("templateId", event.templateId())
                .addValue("version", event.version())
                .addValue("affectedFirms", firmsJson);

        // update() returns the number of affected rows.
        // 1  => we inserted (we own the work)
        // 0  => ON CONFLICT fired (someone else already recorded it)
        int affected = jdbc.update(sql, params);
        return affected == 1;
    }

    /**
     * Update the status of an outbox entry.
     *
     * For FAILED status, increments retry_count and optionally appends error message.
     * The error_message column uses append semantics so we can see the full failure history.
     */
    @Override
    public void updateStatus(Long taskId, UpdateStatus status, String errorMessage) {
        String sql = """
            UPDATE outbox_entries
               SET status = :status,
                   error_message = CASE WHEN :errorMessage::TEXT IS NOT NULL
                                        THEN COALESCE(error_message, '') || ' [' || CURRENT_TIMESTAMP || ': ' || :errorMessage::TEXT || ']'
                                        ELSE error_message END,
                   retry_count = CASE WHEN :status = 'FAILED' THEN retry_count + 1 ELSE retry_count END,
                   updated_at = NOW()
             WHERE task_id = :taskId
            """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("taskId", taskId)
                .addValue("status", status.name())
                .addValue("errorMessage", errorMessage);

        int updated = jdbc.update(sql, params);

        if (updated == 0) {
            throw new IllegalStateException("No outbox entry found for taskId=" + taskId +
                    ". Has it been claimed by another processor?");
        }

        LOGGER.debug("Updated task {} status to {}", taskId, status.name());
    }
}
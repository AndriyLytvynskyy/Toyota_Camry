package com.ebay.challenge.streamprocessor.output;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Output sink that writes attributed page views to SQLite database.
 *
 * Provides idempotent writes using unique constraints on page_view_id.
 * Ensures durability for offset commit safety (at-least-once delivery).
 *
 * TODO: Complete the database write implementation
 */
@Slf4j
@Component
public class OutputSink {

    @Value("${output.database.path:./output/attributed_page_views.db}")
    private String databasePath;

    private Connection connection;
    private PreparedStatement insertStatement;
    private final ObjectMapper objectMapper;
    private final AtomicLong writeCount = new AtomicLong(0);

    public OutputSink() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @PostConstruct
    public void initialize() throws SQLException {
        log.info("Initializing output sink with database: {}", databasePath);

        // Create output directory if needed
        java.io.File dbFile = new java.io.File(databasePath);
        dbFile.getParentFile().mkdirs();

        // Connect to SQLite
        connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
        connection.setAutoCommit(true); // Each write is immediately committed

        // Create table with unique constraint on page_view_id for idempotency
        createTable();

        // Prepare insert statement
        String insertSql = """
            INSERT OR REPLACE INTO attributed_page_views
            (page_view_id, user_id, event_time, url, attributed_campaign_id, attributed_click_id, json_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
        insertStatement = connection.prepareStatement(insertSql);

        log.info("Output sink initialized successfully");
    }

    private void createTable() throws SQLException {
        String createTableSql = """
            CREATE TABLE IF NOT EXISTS attributed_page_views (
                page_view_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                event_time TEXT NOT NULL,
                url TEXT NOT NULL,
                attributed_campaign_id TEXT,
                attributed_click_id TEXT,
                json_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            log.info("Table 'attributed_page_views' created or already exists");
        }
    }

    /**
     * Write an attributed page view to the database.
     * Uses INSERT OR REPLACE for idempotency (same page_view_id won't create duplicates).
     *
     * TODO: Complete the implementation
     * - Serialize the AttributedPageView to JSON
     * - Set all prepared statement parameters
     * - Execute the insert
     * - Increment write counter
     * - Handle errors appropriately
     *
     * @param attributedPageView the attributed page view to write
     */
    public synchronized void write(AttributedPageView attributedPageView) {
        try {
            log.info("Call Output sink");
            // TODO: Serialize the entire object to JSON
            String jsonData = objectMapper.writeValueAsString(attributedPageView);

            // TODO: Set prepared statement parameters
             insertStatement.setString(1, attributedPageView.getPageViewId());
             insertStatement.setString(2, attributedPageView.getUserId());
             insertStatement.setString(3, attributedPageView.getEventTime().toString());
             insertStatement.setString(4, attributedPageView.getUrl());
             insertStatement.setString(5, attributedPageView.getAttributedCampaignId());
             insertStatement.setString(6, attributedPageView.getAttributedClickId());
             insertStatement.setString(7, jsonData);

            // TODO: Execute the insert
             insertStatement.executeUpdate();
             long count = writeCount.incrementAndGet();

            log.debug("Written attributed page view: {} (total writes: {})",
                attributedPageView.getPageViewId(), count);

        } catch (Exception e) {
            log.error("Failed to write attributed page view: {}", attributedPageView.getPageViewId(), e);
            throw new RuntimeException("Database write failed", e);
        }
    }

    /**
     * Get the total number of writes performed.
     *
     * @return total write count
     */
    public long getWriteCount() {
        return writeCount.get();
    }

    @PreDestroy
    public void close() {
        try {
            if (insertStatement != null) {
                insertStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
            log.info("Output sink closed (total writes: {})", writeCount.get());
        } catch (SQLException e) {
            log.error("Error closing output sink", e);
        }
    }
}

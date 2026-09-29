package com.example.kafkatest;

import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class DiagnosticDataLoaderTest {
    @Test
    public void contentHasSampleLengthAndIsUniqueWithinBatch() {
        Set<String> contents = new HashSet<>();
        for (int index = 1; index <= 118; index++) {
            String content = DiagnosticDataLoader.contentFor(index);
            assertEquals(DiagnosticDataLoader.SAMPLE_CONTENT.length(), content.length());
            assertEquals(0, content.length() % 2);
            assertTrue(content.matches("[0-9A-F]+"));
            assertTrue(contents.add(content));
        }
        assertEquals(118, contents.size());
        assertEquals(DiagnosticDataLoader.SAMPLE_CONTENT.length(),
                DiagnosticDataLoader.contentFor(Integer.MAX_VALUE).length());
    }

    @Test
    public void filenameChangesOnlyTimestamp() {
        LocalDateTime timestamp = LocalDateTime.of(2026, 9, 18, 15, 53, 14);
        assertEquals("20260918155314_118_1_1303_X_26667_1.csv",
                DiagnosticDataLoader.filenameFor(timestamp));
        assertEquals("20260918155315_118_1_1303_X_26667_1.csv",
                DiagnosticDataLoader.filenameFor(timestamp.plusSeconds(1)));
    }

    @Test
    public void defaultBatchMatchesSampleAndCommitsOnce() throws SQLException {
        FakeJdbc jdbc = new FakeJdbc();
        DiagnosticDataLoader.Options options = DiagnosticDataLoader.Options.parse(new String[0]);
        options.jdbcBatchSize = 50;
        String filename = "20260918155314_118_1_1303_X_26667_1.csv";
        DiagnosticDataLoader.insertBatch(jdbc.connection(), options, filename);
        assertEquals(Arrays.asList(50, 50, 18), jdbc.batchSizes);
        assertEquals(1, jdbc.commits);
        assertEquals(0, jdbc.rollbacks);
        assertEquals(118, jdbc.rows.size());
        Set<String> contents = new HashSet<>();
        for (int i = 0; i < jdbc.rows.size(); i++) {
            Object[] row = jdbc.rows.get(i);
            assertEquals("EE6CDA86-DCFB-4165-9CE7-C8CEED9FF656", row[1]);
            assertEquals("25\u6d4b888888", row[2]);
            assertEquals(53334, row[3]);
            assertEquals(118, row[4]);
            assertEquals(i + 1, row[5]);
            assertTrue(contents.add((String) row[6]));
            assertEquals(filename, row[7]);
        }
    }

    @Test
    public void customPacketCountResetsIndexForEachFile() throws SQLException {
        FakeJdbc jdbc = new FakeJdbc();
        DiagnosticDataLoader.Options options = DiagnosticDataLoader.Options.parse(
                new String[]{"--packet-count", "3", "--jdbc-batch-size", "2"});
        DiagnosticDataLoader.insertBatch(jdbc.connection(), options, "first.csv");
        DiagnosticDataLoader.insertBatch(jdbc.connection(), options, "second.csv");
        assertEquals(6, jdbc.rows.size());
        assertEquals(2, jdbc.commits);
        assertEquals(Arrays.asList(2, 1, 2, 1), jdbc.batchSizes);
        for (int i = 0; i < 6; i++) {
            assertEquals(3, jdbc.rows.get(i)[4]);
            assertEquals(i % 3 + 1, jdbc.rows.get(i)[5]);
            assertEquals(i < 3 ? "first.csv" : "second.csv", jdbc.rows.get(i)[7]);
        }
    }

    @Test
    public void singlePacketStillExecutesAndCommits() throws SQLException {
        FakeJdbc jdbc = new FakeJdbc();
        DiagnosticDataLoader.Options options = DiagnosticDataLoader.Options.parse(
                new String[]{"--packet-count", "1"});
        DiagnosticDataLoader.insertBatch(jdbc.connection(), options, "one.csv");
        assertEquals(Arrays.asList(1), jdbc.batchSizes);
        assertEquals(1, jdbc.commits);
    }

    @Test
    public void failureAfterFirstChunkRollsBackWholeFile() throws SQLException {
        FakeJdbc jdbc = new FakeJdbc();
        jdbc.failOnExecution = 2;
        DiagnosticDataLoader.Options options = new DiagnosticDataLoader.Options();
        options.jdbcBatchSize = 50;
        try {
            DiagnosticDataLoader.insertBatch(jdbc.connection(), options, "failed.csv");
            fail("Expected SQLException");
        } catch (SQLException expected) {
            assertEquals("Simulated insert failure", expected.getMessage());
        }
        assertEquals(0, jdbc.commits);
        assertEquals(1, jdbc.rollbacks);
        assertEquals(Arrays.asList(50, 50), jdbc.batchSizes);
    }

    @Test
    public void driverFailureResultAlsoRollsBack() throws SQLException {
        FakeJdbc jdbc = new FakeJdbc();
        jdbc.failedResult = true;
        try {
            DiagnosticDataLoader.insertBatch(jdbc.connection(), new DiagnosticDataLoader.Options(), "failed.csv");
            fail("Expected SQLException");
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().contains("failed insert"));
        }
        assertEquals(0, jdbc.commits);
        assertEquals(1, jdbc.rollbacks);
    }

    @Test
    public void rejectsInvalidOptions() {
        for (String[] args : new String[][]{
                {"--packet-count", "0"}, {"--batches", "-1"}, {"--jdbc-batch-size", "0"},
                {"--interval-ms", "-1"}, {"--batches", "2147483648"},
                {"--packet-count", "abc"}, {"--packet-count"}, {"--unknown", "1"}}) {
            try {
                DiagnosticDataLoader.Options.parse(args);
                fail("Should reject " + Arrays.toString(args));
            } catch (IllegalArgumentException expected) {
                assertNotNull(expected.getMessage());
            }
        }
    }

    private static final class FakeJdbc {
        final List<Object[]> rows = new ArrayList<>();
        final List<Integer> batchSizes = new ArrayList<>();
        final Object[] parameters = new Object[8];
        int pending;
        int commits;
        int rollbacks;
        int failOnExecution;
        boolean failedResult;

        Connection connection() {
            PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "setInt":
                            case "setString":
                            case "setNString":
                                parameters[(Integer) args[0]] = args[1];
                                return null;
                            case "addBatch":
                                rows.add(parameters.clone());
                                pending++;
                                return null;
                            case "executeBatch":
                                batchSizes.add(pending);
                                if (batchSizes.size() == failOnExecution) {
                                    throw new SQLException("Simulated insert failure");
                                }
                                int[] results = new int[pending];
                                Arrays.fill(results, failedResult ? Statement.EXECUTE_FAILED : 1);
                                return results;
                            case "clearBatch":
                                pending = 0;
                                return null;
                            case "close":
                                return null;
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "prepareStatement":
                                assertEquals(DiagnosticDataLoader.INSERT_SQL, args[0]);
                                return statement;
                            case "commit":
                                commits++;
                                return null;
                            case "rollback":
                                rollbacks++;
                                return null;
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }
    }
}

package com.example.kafkatest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public final class DiagnosticDataLoader {
    static final String ELEVATOR_ID = "EE6CDA86-DCFB-4165-9CE7-C8CEED9FF656";
    static final String DEVICE_NO = "25\u6d4b888888";
    static final int DATA_LENGTH = 53334;
    static final String SAMPLE_CONTENT =
            "2208F0071D080F08F4073308FB072208D90716081308290810080908FB07280822080008E9075108EF0717080C082B08ED071B08FC072208040832080A080208030805080708EA0704081D08F50710081E08F0070908EA071A08FF075308310802080E082B0808080C080B08060802081708060810082208D2072B0804080108EC073808EE073208EE07080829082E08FB07E8072308EF07F207FC071F081C08E4072E080908250806080A081A080C0829083308FD073608F5071608E4070F083C08FB07380840081A0817080508000833082808E60701081C08DB07FB07FE072008F6070108FF071308FA072308F7071D08ED072808F00712080708FB07E0070608FB07F4071808FF071008E907DC07FC072808FC07EE07030817082E083408DD071408C8071808EC072208F9071C081A081908EE0708083F08ED070608F6072008EB073308E407040824082508FF07FE0710083A081508250806081208F007F5071008ED07350821082908F807FB07100800083808140816082E083C080E080D08F3070A080B080C08FB0735082E083408EC071E08060807083F0819082908F50723081108F3073D08ED071308D1073708010802081708F807F30711080E0803081B08";
    static final String INSERT_SQL = "INSERT INTO [dbo].[DiagnosticData] "
            + "(elevatorid, deviceno, datalength, packetcount, packetindex, content, filename) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final String DEFAULT_URL = "jdbc:sqlserver://10.96.128.166:1433;"
            + "databaseName=ETBase;encrypt=true;trustServerCertificate=false;loginTimeout=15";

    private DiagnosticDataLoader() {
    }

    public static void main(String[] args) {
        try {
            Options options = Options.parse(args);
            if (options.help) {
                System.out.println("Usage: java -jar diagnostic-data-loader.jar [options]\n"
                        + "  --batches N          File batches to insert (default: 1)\n"
                        + "  --packet-count N     Rows per file batch (default: 118)\n"
                        + "  --jdbc-batch-size N  Rows per JDBC executeBatch (default: 500)\n"
                        + "  --interval-ms N      Pause between file batches (default: 0)\n"
                        + "  --dry-run            Preview generation without database access\n"
                        + "  --help               Show this help\n"
                        + "Database: DIAGNOSTIC_DB_URL (optional), DIAGNOSTIC_DB_USER, DIAGNOSTIC_DB_PASSWORD");
                return;
            }
            run(options);
        } catch (Exception error) {
            System.err.println("Load failed: " + error.getMessage());
            System.err.println("Previously committed file batches remain in the database. No automatic retry.");
            System.exit(1);
        }
    }

    static String contentFor(int packetIndex) {
        if (packetIndex < 1) {
            throw new IllegalArgumentException("packetIndex must be positive");
        }
        // A fixed-width index guarantees uniqueness without changing the payload length.
        return SAMPLE_CONTENT.substring(0, SAMPLE_CONTENT.length() - 8)
                + String.format(Locale.ROOT, "%08X", packetIndex);
    }

    static String filenameFor(LocalDateTime timestamp) {
        return timestamp.format(TIMESTAMP) + "_118_1_1303_X_26667_1.csv";
    }

    private static void run(Options options) throws SQLException, InterruptedException {
        System.out.printf(Locale.ROOT,
                "Mode=%s, batches=%d, packetcount=%d, totalRows=%d, contentHexLength=%d%n",
                options.dryRun ? "PREVIEW" : "INSERT", options.batches, options.packetCount,
                (long) options.batches * options.packetCount, SAMPLE_CONTENT.length());
        if (options.dryRun) {
            runBatches(null, options);
            return;
        }
        String user = requiredEnv("DIAGNOSTIC_DB_USER");
        String password = requiredEnv("DIAGNOSTIC_DB_PASSWORD");
        String url = System.getenv("DIAGNOSTIC_DB_URL");
        if (url == null || url.trim().isEmpty()) {
            url = DEFAULT_URL;
        }
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            connection.setAutoCommit(false);
            runBatches(connection, options);
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing environment variable: " + name);
        }
        return value;
    }

    private static void runBatches(Connection connection, Options options)
            throws SQLException, InterruptedException {
        LocalDateTime timestamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        long started = System.nanoTime();
        long totalRows = 0;
        for (int batch = 0; batch < options.batches; batch++) {
            String filename = filenameFor(timestamp);
            if (options.dryRun) {
                System.out.printf(Locale.ROOT, "Preview batch=%d filename=%s packetindex=1..%d%n",
                        batch + 1, filename, options.packetCount);
                System.out.println("  firstContent=" + contentFor(1));
                System.out.println("  lastContent=" + contentFor(options.packetCount));
            } else {
                insertBatch(connection, options, filename);
                totalRows += options.packetCount;
                double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
                System.out.printf(Locale.ROOT,
                        "Committed batch=%d filename=%s rows=%d totalRows=%d averageRowsPerSecond=%.2f%n",
                        batch + 1, filename, options.packetCount, totalRows, totalRows / seconds);
            }
            if (batch < options.batches - 1) {
                if (options.intervalMs > 0) {
                    Thread.sleep(options.intervalMs);
                }
                // Consecutive batches can finish within one second; still assign different filenames.
                LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
                timestamp = now.isAfter(timestamp) ? now : timestamp.plusSeconds(1);
            }
        }
    }

    static void insertBatch(Connection connection, Options options, String filename) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            statement.setString(1, ELEVATOR_ID);
            statement.setNString(2, DEVICE_NO);
            statement.setInt(3, DATA_LENGTH);
            statement.setInt(4, options.packetCount);
            statement.setString(7, filename);
            int pending = 0;
            for (int offset = 0; offset < options.packetCount; offset++) {
                int packetIndex = offset + 1;
                statement.setInt(5, packetIndex);
                statement.setString(6, contentFor(packetIndex));
                statement.addBatch();
                pending++;
                if (pending == options.jdbcBatchSize || packetIndex == options.packetCount) {
                    for (int result : statement.executeBatch()) {
                        if (result == Statement.EXECUTE_FAILED) {
                            throw new SQLException("JDBC batch reported a failed insert");
                        }
                    }
                    statement.clearBatch();
                    pending = 0;
                }
            }
        } catch (SQLException | RuntimeException error) {
            rollback(connection, error);
            throw error;
        }
        try {
            connection.commit();
        } catch (SQLException error) {
            rollback(connection, error);
            throw new SQLException("Commit could not be confirmed for " + filename
                    + "; verify database state before retrying", error);
        }
    }

    private static void rollback(Connection connection, Exception error) {
        try {
            connection.rollback();
        } catch (SQLException rollbackError) {
            error.addSuppressed(rollbackError);
        }
    }

    static final class Options {
        int batches = 1;
        int packetCount = 118;
        int jdbcBatchSize = 500;
        long intervalMs;
        boolean dryRun;
        boolean help;

        static Options parse(String[] args) {
            Options options = new Options();
            for (int i = 0; i < args.length; i++) {
                String option = args[i];
                if ("--dry-run".equals(option)) {
                    options.dryRun = true;
                } else if ("--help".equals(option)) {
                    options.help = true;
                } else {
                    if (!"--batches".equals(option) && !"--packet-count".equals(option)
                            && !"--jdbc-batch-size".equals(option) && !"--interval-ms".equals(option)) {
                        throw new IllegalArgumentException("Unknown option: " + option);
                    }
                    if (++i >= args.length) {
                        throw new IllegalArgumentException("Missing value for " + option);
                    }
                    long value;
                    try {
                        value = Long.parseLong(args[i]);
                    } catch (NumberFormatException error) {
                        throw new IllegalArgumentException("Invalid integer for " + option + ": " + args[i]);
                    }
                    if ("--interval-ms".equals(option)) {
                        if (value < 0) {
                            throw new IllegalArgumentException("--interval-ms must be nonnegative");
                        }
                        options.intervalMs = value;
                    } else {
                        if (value < 1 || value > Integer.MAX_VALUE) {
                            throw new IllegalArgumentException(option + " must be between 1 and " + Integer.MAX_VALUE);
                        }
                        if ("--batches".equals(option)) {
                            options.batches = (int) value;
                        } else if ("--packet-count".equals(option)) {
                            options.packetCount = (int) value;
                        } else {
                            options.jdbcBatchSize = (int) value;
                        }
                    }
                }
            }
            return options;
        }
    }
}

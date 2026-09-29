# Java SQL Server load generator

Requires JDK 8+ and Maven. This standalone tool inserts into SQL Server so an
existing database-to-Kafka pipeline can be load tested. It does not publish to
Kafka itself. The existing C# producer is independent.

## Build and preview

Run from the workspace root in PowerShell:

```powershell
mvn -f .\java-loader\pom.xml clean package
java -jar .\java-loader\target\diagnostic-data-loader.jar --batches 2 --dry-run
```

The executable JAR includes the SQL Server JDBC driver. Preview requires no
credentials or database access and prints the first and last content per file.

When Maven plugins cannot be downloaded, use an already available JDBC driver
and the JDK directly (no network access):

```powershell
.\java-loader\build-local.ps1
# Or select your own local driver:
.\java-loader\build-local.ps1 -JdbcJar 'C:\drivers\mssql-jdbc-12.8.1.jre8.jar'
java -jar .\java-loader\target\diagnostic-data-loader.jar --batches 2 --dry-run
```

The script defaults to version `12.8.1.jre8` in the user's local Maven cache.
Unlike the Maven build, this build keeps the driver separately in
`target/lib/mssql-jdbc.jar`; keep that directory alongside the application JAR.

## Insert

Set credentials for an account allowed to insert into `ETBase.dbo.DiagnosticData`:

```powershell
$env:DIAGNOSTIC_DB_USER = 'your_database_user'
$credential = Get-Credential -UserName $env:DIAGNOSTIC_DB_USER -Message 'SQL Server credentials'
$env:DIAGNOSTIC_DB_PASSWORD = $credential.GetNetworkCredential().Password
$env:DIAGNOSTIC_DB_URL = 'jdbc:sqlserver://10.96.128.166:1433;databaseName=ETBase;encrypt=true;trustServerCertificate=false;loginTimeout=15'

# 100 files x 118 packets = 11,800 rows
java -jar .\java-loader\target\diagnostic-data-loader.jar --batches 100
```

The URL above is the default and can be omitted. Adjust host, port and TLS settings
for your SQL Server deployment. When the server uses an untrusted self-signed
certificate, install its CA certificate in the JVM trust store. For an isolated
test environment, `trustServerCertificate=true` skips certificate verification.

The connection targets `10.96.128.166` directly and selects database `ETBase`, so
the INSERT uses `[dbo].[DiagnosticData]` instead of a four-part linked-server name.
No table is created or cleared. Successful runs exit with code 0; failures exit
with code 1 and stop further batches.

## Generation rules

| Field | Value |
| --- | --- |
| elevatorid | `EE6CDA86-DCFB-4165-9CE7-C8CEED9FF656` |
| deviceno | `25\u6d4b888888` (the sample's Chinese character, sent as Unicode) |
| datalength | `53334`, unchanged even when packet count changes |
| packetcount | `118` by default; `--packet-count` changes both this field and rows per file |
| packetindex | `1` through `packetcount`, reset for each new file |
| content | 904 hex characters (452 bytes), same as the sample; last 8 hex characters encode packetindex |
| filename | `yyyyMMddHHmmss_118_1_1303_X_26667_1.csv`, fixed throughout one file batch |

Content is deterministic, uppercase hex, and guaranteed distinct within one file
batch. Different file batches reuse the same content sequence. The sample prefix
is retained, with only the final 4 bytes replaced by the packet index. This is
synthetic test data, not a recording of real device measurements.

File timestamps start at local system time. The next timestamp is the later of
current local time and the previous timestamp plus one second, so fast inserts
still produce distinct file names without throttling. Timestamps may run ahead
of wall time. Names are unique within one run; simultaneous runs or rapid restarts
can reuse names. The suffix stays exactly as supplied even with a custom packet
count. Underscores are literal `_`, with no backslashes.

## Controls and transaction behavior

```powershell
java -jar .\java-loader\target\diagnostic-data-loader.jar --batches 1000 --packet-count 118 --jdbc-batch-size 50 --interval-ms 100
java -jar .\java-loader\target\diagnostic-data-loader.jar --help
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--batches` | `1` | Number of complete files to insert |
| `--packet-count` | `118` | Rows per complete file |
| `--jdbc-batch-size` | `500` | Maximum rows per JDBC executeBatch call |
| `--interval-ms` | `0` | Delay between files, not between individual rows |
| `--dry-run` | off | Preview generation without database access |

One file is one transaction. For example, 118 packets with JDBC batch size 50
execute as 50 + 50 + 18 rows, then commit together. An insertion failure rolls
back the current file; earlier committed files remain. There is no automatic
retry. If commit confirmation fails because the connection is lost, inspect the
database before retrying because the server may already have committed.

Output includes committed rows and average rows/second, including generation,
database operations, and configured pauses. It does not measure Kafka delivery
or consumer throughput; check those separately in the existing pipeline.

## Tests

```powershell
mvn -f .\java-loader\pom.xml test
```

Tests cover payload length and uniqueness, filenames, parameter binding,
packet index reset, partial JDBC batches, rollback behavior and invalid options.
JDBC tests use a local fake connection; no database is contacted.

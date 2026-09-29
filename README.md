# KafkaTest

For the standalone Java SQL Server batch inserter, see [java-loader/README.md](java-loader/README.md).
It generates DiagnosticData file batches for testing an existing database-to-Kafka pipeline.

Requires the .NET 8 SDK and a reachable Kafka broker. No database is used.

```powershell
dotnet run --project KafkaTest.csproj
```

The default bootstrap servers are
`10.96.129.70:9092,10.96.129.71:9092,10.98.254.36:9092,10.98.254.37:9092,10.98.254.38:9092`.
To override them, pass one quoted argument after `--`: `"host1:9092,host2:9092"`.

The program sends one batch of 10 identical JSON messages to `escalator_file_test`,
waits for every send to succeed or fail, disposes the producer, and exits automatically.
The supplied file name, packet fields, and payload are identical in all
10 messages; the IDs 1 through 10 are used only for logging and the in-memory
success dictionary. Kafka idempotence does not remove these intentional duplicates.

The previous `--once` argument is still accepted but is no longer required.

If the SDK was installed locally in this project, replace `dotnet` with
`.\.tools\dotnet\dotnet.exe`.

`test.txt` is the original source snippet. `Program.cs` contains the runnable
version. Producer settings use `Acks.All`, 5 send retries, idempotence enabled,
and a 30000 ms request timeout. Error logging is preserved. `PushDiagnostic`
returns `Task` so the batch can wait for all sends before disposing the producer.
Delivery errors are logged; they do not cause a nonzero process exit code.

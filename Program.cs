// 改进代码

using System.Collections.Concurrent;

using Confluent.Kafka;



internal static class Program

{

    private static string kafkaServer = "10.96.129.70:9092,10.96.129.71:9092,10.98.254.36:9092,10.98.254.37:9092,10.98.254.38:9092";

    private static readonly ConcurrentDictionary<string, DateTime> gDiagnosticIds = new();



    private static void Main(string[] args)

    {

        if (args.Contains("--help"))

        {

            Console.WriteLine("Usage: KafkaTest [bootstrap-servers]");

            Console.WriteLine("Default servers: " + kafkaServer);

            Console.WriteLine("Topic: escalator_file_test, sends 10 messages and exits after delivery results.");

            return;

        }



        kafkaServer = args.FirstOrDefault(arg => arg != "--once") ?? kafkaServer;

        LogHelper.Write("Kafka server: " + kafkaServer);

        ProcessDiagnostic();

    }



    private async static Task PushDiagnostic(string topic, string id, IProducer<string, string> p)

    {

        try

        {

            string data = @"{""file_name"":""20260911095202_0000_4_1.json"",""data_size"":342,""buff_count"":1,""buff_index"":1,""buff_bytes"":""565330313030393837363534333434000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"",""deviceno"":""25R016240""}";



            LogHelper.Write("PushDiagnostic " + topic + " " + id + " " + data);



            var result = await p.ProduceAsync(topic, new Message<string, string> { Value = data });



            LogHelper.Write("PushDiagnostic " + topic + " " + id + " result position:" + result.TopicPartitionOffset);



            if (!gDiagnosticIds.ContainsKey(id))

            {

                gDiagnosticIds.TryAdd(id, DateTime.Now);

            }

        }

        catch (Exception ex)

        {

            LogHelper.Write("PushDiagnostic Error:" + ex.Message + "," + ex.StackTrace);

        }

    }



    public static void ProcessDiagnostic()

    {

        try

        {

            var config = new ProducerConfig
            {
                BootstrapServers = kafkaServer,
                Acks = Acks.All,
                MessageSendMaxRetries = 5,
                EnableIdempotence = true,
                RequestTimeoutMs = 60000
                // 从默认 100ms 加到 1 秒。
                // ReconnectBackoffMs = 1000,
                // ReconnectBackoffMaxMs = 10000,
                // // 开启 TCP KeepAlive，让客户端和 Kafka broker 的连接在空闲时也能定期探测是否还活着
                // SocketKeepaliveEnable = true,
                // // 从默认 100ms 加到 1 秒。
                // RetryBackoffMs = 1000
            };



            using (var p = new ProducerBuilder<string, string>(config).Build())

            {

                p.Flush();



                LogHelper.Write("Diagnostic count:10");

                // [新增] 收集所有发送任务，便于在释放 Producer 前统一等待。

                var tasks = new List<Task>();

                for (int j = 0; j < 500; j++)

                {

                    string id = (j + 1).ToString();

                    tasks.Add(PushDiagnostic("escalator_file_test", id, p));

                }



                // [新增] 在 using 内等待所有任务结束，再释放 Producer，避免发送中途释放资源。

                Task.WhenAll(tasks).GetAwaiter().GetResult();

            }

        }

        catch (Exception ex)

        {

            LogHelper.Write("ProcessDiagnostic Error:" + ex.Message + "," + ex.StackTrace);

        }
        Thread.Sleep(1000 * 300);

    }

}

// [新增] 为原片段使用的 LogHelper 补齐控制台实现，每行附带本机时间。

internal static class LogHelper

{

    public static void Write(string message)

    {
        Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + " " + message);

    }

}

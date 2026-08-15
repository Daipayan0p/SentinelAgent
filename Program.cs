using System.Diagnostics.Eventing.Reader;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SentinelAgent;

internal class Program
{
    private static AgentConfig _config = new();

    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(15)
    };

    private static readonly List<EventLogWatcher> Watchers = new();

    private static readonly Queue<WindowsLogEvent> LogQueue = new();

    private static readonly object QueueLock = new();

    private static readonly CancellationTokenSource Cancellation =
        new();

    private static long TotalCollected;
    private static long TotalSent;
    private static long TotalFailed;

    private static async Task Main()
    {
        Console.Title = "SentinelAgent";

        PrintBanner();

        if (!LoadConfiguration())
        {
            Console.WriteLine();
            Console.WriteLine("SentinelAgent could not start.");
            return;
        }

        Console.WriteLine(
            $"[CONFIG] Server: {_config.ServerUrl}");

        Console.WriteLine(
            $"[CONFIG] Batch size: {_config.BatchSize}");

        Console.WriteLine(
            $"[CONFIG] Send interval: {_config.SendIntervalSeconds}s");

        Console.WriteLine();

        if (!Uri.TryCreate(
                _config.ServerUrl,
                UriKind.Absolute,
                out Uri? serverUri))
        {
            Console.WriteLine(
                "[ERROR] Invalid serverUrl in config.json.");

            return;
        }

        if (serverUri.Scheme != Uri.UriSchemeHttp &&
            serverUri.Scheme != Uri.UriSchemeHttps)
        {
            Console.WriteLine(
                "[ERROR] serverUrl must use HTTP or HTTPS.");

            return;
        }

        StartEventLogCollection();

        Console.WriteLine();
        Console.WriteLine(
            $"[READY] Monitoring {Watchers.Count} Windows Event Log channels.");

        Console.WriteLine(
            "[READY] SentinelAgent is running.");

        Console.WriteLine();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;

            Cancellation.Cancel();

            StopWatchers();
        };

        await RunSenderLoop(Cancellation.Token);
    }

    // ============================================================
    // BANNER
    // ============================================================

    private static void PrintBanner()
    {
        Console.WriteLine(
            "==================================================");

        Console.WriteLine(
            "                 SENTINELAGENT");

        Console.WriteLine(
            "          Windows Log Collection Agent");

        Console.WriteLine(
            "==================================================");

        Console.WriteLine();
    }

    // ============================================================
    // CONFIGURATION
    // ============================================================

    private static bool LoadConfiguration()
    {
        try
        {
            string configPath = Path.Combine(
                AppContext.BaseDirectory,
                "config.json");

            if (!File.Exists(configPath))
            {
                CreateDefaultConfiguration(configPath);

                Console.WriteLine(
                    "[CONFIG] config.json was not found.");

                Console.WriteLine(
                    "[CONFIG] A new config.json was created.");

                Console.WriteLine(
                    "[CONFIG] Edit it and start SentinelAgent again.");

                return false;
            }

            string json = File.ReadAllText(configPath);

            AgentConfig? config =
                JsonSerializer.Deserialize<AgentConfig>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

            if (config == null)
            {
                Console.WriteLine(
                    "[ERROR] Could not parse config.json.");

                return false;
            }

            _config = config;

            if (string.IsNullOrWhiteSpace(
                    _config.ServerUrl))
            {
                Console.WriteLine(
                    "[ERROR] serverUrl cannot be empty.");

                return false;
            }

            if (_config.BatchSize <= 0)
            {
                _config.BatchSize = 50;
            }

            if (_config.SendIntervalSeconds <= 0)
            {
                _config.SendIntervalSeconds = 5;
            }

            if (_config.MaxBufferedLogs <= 0)
            {
                _config.MaxBufferedLogs = 10000;
            }

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"[ERROR] Config error: {ex.Message}");

            return false;
        }
    }

    private static void CreateDefaultConfiguration(
        string path)
    {
        var config = new AgentConfig
        {
            ServerUrl =
                "http://localhost:8080/api/logs",

            BatchSize = 50,

            SendIntervalSeconds = 5,

            MaxBufferedLogs = 10000
        };

        string json = JsonSerializer.Serialize(
            config,
            new JsonSerializerOptions
            {
                WriteIndented = true
            });

        File.WriteAllText(path, json);
    }

    // ============================================================
    // EVENT LOG COLLECTION
    // ============================================================

    private static void StartEventLogCollection()
    {
        Console.WriteLine(
            "[INIT] Discovering Windows Event Logs...");

        try
        {
            using EventLogSession session =
                new();

            IEnumerable<string> logNames =
                session.GetLogNames();

            foreach (string logName in logNames)
            {
                StartWatcher(logName);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"[ERROR] Could not enumerate Event Logs: {ex.Message}");
        }
    }

    private static void StartWatcher(string channel)
    {
        try
        {
            EventLogQuery query =
                new EventLogQuery(
                    channel,
                    PathType.LogName,
                    "*[System[*]]");

            EventLogWatcher watcher =
                new(query);

            watcher.EventRecordWritten +=
                OnEventRecordWritten;

            watcher.Enabled = true;

            lock (Watchers)
            {
                Watchers.Add(watcher);
            }

            Console.WriteLine(
                $"[WATCH] {channel}");
        }
        catch
        {
            // Some Windows logs are inaccessible without
            // administrator privileges.
        }
    }

    // ============================================================
    // EVENT RECEIVED
    // ============================================================

    private static void OnEventRecordWritten(
        object? sender,
        EventRecordWrittenEventArgs args)
    {
        if (args.EventRecord == null)
        {
            return;
        }

        try
        {
            EventRecord record =
                args.EventRecord;

            string message = string.Empty;

            try
            {
                message =
                    record.FormatDescription()
                    ?? string.Empty;
            }
            catch
            {
                // Some events cannot be formatted.
            }

            var log = new WindowsLogEvent
            {
                AgentId = AgentIdentity.Id,

                Hostname =
                    Environment.MachineName,

                Channel =
                    record.LogName ?? string.Empty,

                Provider =
                    record.ProviderName ?? string.Empty,

                EventId =
                    record.Id,

                Level =
                    record.LevelDisplayName ?? string.Empty,

                TimeCreated =
                    record.TimeCreated,

                RecordId =
                    record.RecordId,

                ProcessId =
                    record.ProcessId,

                ThreadId =
                    record.ThreadId,

                MachineName =
                    record.MachineName ?? string.Empty,

                Message =
                    message,

                RawXml =
                    record.ToXml()
            };

            AddToQueue(log);

            Interlocked.Increment(
                ref TotalCollected);
        }
        catch
        {
            // Never allow a single malformed event
            // to kill the collector.
        }
    }

    // ============================================================
    // QUEUE
    // ============================================================

    private static void AddToQueue(
        WindowsLogEvent log)
    {
        lock (QueueLock)
        {
            while (LogQueue.Count >=
                   _config.MaxBufferedLogs)
            {
                LogQueue.Dequeue();
            }

            LogQueue.Enqueue(log);
        }
    }

    private static List<WindowsLogEvent>
        GetBatch(int amount)
    {
        var batch =
            new List<WindowsLogEvent>();

        lock (QueueLock)
        {
            while (batch.Count < amount &&
                   LogQueue.Count > 0)
            {
                batch.Add(
                    LogQueue.Dequeue());
            }
        }

        return batch;
    }

    private static void ReturnBatch(
        List<WindowsLogEvent> batch)
    {
        lock (QueueLock)
        {
            var existing =
                LogQueue.ToList();

            LogQueue.Clear();

            foreach (WindowsLogEvent log in batch)
            {
                if (LogQueue.Count >=
                    _config.MaxBufferedLogs)
                {
                    break;
                }

                LogQueue.Enqueue(log);
            }

            foreach (WindowsLogEvent log in existing)
            {
                if (LogQueue.Count >=
                    _config.MaxBufferedLogs)
                {
                    break;
                }

                LogQueue.Enqueue(log);
            }
        }
    }

    // ============================================================
    // SEND LOOP
    // ============================================================

    private static async Task RunSenderLoop(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await SendAvailableLogs(
                    cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"[SEND ERROR] {ex.Message}");
            }

            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(
                        _config.SendIntervalSeconds),
                    cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    // ============================================================
    // HTTP SENDING
    // ============================================================

    private static async Task SendAvailableLogs(
        CancellationToken cancellationToken)
    {
        while (true)
        {
            List<WindowsLogEvent> batch =
                GetBatch(_config.BatchSize);

            if (batch.Count == 0)
            {
                return;
            }

            var payload = new LogBatch
            {
                AgentId =
                    AgentIdentity.Id,

                Hostname =
                    Environment.MachineName,

                OperatingSystem =
                    Environment.OSVersion
                        .VersionString,

                SentAt =
                    DateTime.UtcNow,

                Logs =
                    batch
            };

            try
            {
                HttpResponseMessage response =
                    await HttpClient.PostAsJsonAsync(
                        _config.ServerUrl,
                        payload,
                        cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    Interlocked.Add(
                        ref TotalSent,
                        batch.Count);

                    Console.WriteLine(
                        $"[SENT] {batch.Count} logs | " +
                        $"Queue: {GetQueueSize()} | " +
                        $"Total Sent: {TotalSent}");
                }
                else
                {
                    Interlocked.Increment(
                        ref TotalFailed);

                    Console.WriteLine(
                        $"[SERVER ERROR] " +
                        $"{(int)response.StatusCode} " +
                        $"{response.StatusCode}");

                    ReturnBatch(batch);

                    return;
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(
                    ref TotalFailed);

                Console.WriteLine(
                    $"[NETWORK] Server unavailable: " +
                    $"{ex.Message}");

                ReturnBatch(batch);

                return;
            }
        }
    }

    // ============================================================
    // QUEUE SIZE
    // ============================================================

    private static int GetQueueSize()
    {
        lock (QueueLock)
        {
            return LogQueue.Count;
        }
    }

    // ============================================================
    // STOP
    // ============================================================

    private static void StopWatchers()
    {
        Console.WriteLine();
        Console.WriteLine(
            "[STOP] Stopping SentinelAgent...");

        lock (Watchers)
        {
            foreach (EventLogWatcher watcher
                     in Watchers)
            {
                try
                {
                    watcher.Enabled = false;
                    watcher.Dispose();
                }
                catch
                {
                    // Ignore shutdown errors.
                }
            }

            Watchers.Clear();
        }

        Console.WriteLine(
            "[STOP] SentinelAgent stopped.");
    }
}

// ================================================================
// CONFIGURATION
// ================================================================

public class AgentConfig
{
    [JsonPropertyName("serverUrl")]
    public string ServerUrl { get; set; } = "";

    [JsonPropertyName("batchSize")]
    public int BatchSize { get; set; } = 50;

    [JsonPropertyName("sendIntervalSeconds")]
    public int SendIntervalSeconds { get; set; } = 5;

    [JsonPropertyName("maxBufferedLogs")]
    public int MaxBufferedLogs { get; set; } = 10000;
}

// ================================================================
// SERVER PAYLOAD
// ================================================================

public class LogBatch
{
    [JsonPropertyName("agentId")]
    public string AgentId { get; set; } = "";

    [JsonPropertyName("hostname")]
    public string Hostname { get; set; } = "";

    [JsonPropertyName("operatingSystem")]
    public string OperatingSystem { get; set; } = "";

    [JsonPropertyName("sentAt")]
    public DateTime SentAt { get; set; }

    [JsonPropertyName("logs")]
    public List<WindowsLogEvent> Logs { get; set; } = new();
}

// ================================================================
// WINDOWS LOG
// ================================================================

public class WindowsLogEvent
{
    [JsonPropertyName("agentId")]
    public string AgentId { get; set; } = "";

    [JsonPropertyName("hostname")]
    public string Hostname { get; set; } = "";

    [JsonPropertyName("machineName")]
    public string MachineName { get; set; } = "";

    [JsonPropertyName("channel")]
    public string Channel { get; set; } = "";

    [JsonPropertyName("provider")]
    public string Provider { get; set; } = "";

    [JsonPropertyName("eventId")]
    public int EventId { get; set; }

    [JsonPropertyName("level")]
    public string Level { get; set; } = "";

    [JsonPropertyName("timeCreated")]
    public DateTime? TimeCreated { get; set; }

    [JsonPropertyName("recordId")]
    public long? RecordId { get; set; }

    [JsonPropertyName("processId")]
    public int? ProcessId { get; set; }

    [JsonPropertyName("threadId")]
    public int? ThreadId { get; set; }

    [JsonPropertyName("message")]
    public string Message { get; set; } = "";

    [JsonPropertyName("rawXml")]
    public string RawXml { get; set; } = "";
}

// ================================================================
// MACHINE IDENTITY
// ================================================================

public static class AgentIdentity
{
    private static readonly string FilePath =
        Path.Combine(
            Environment.GetFolderPath(
                Environment.SpecialFolder.CommonApplicationData),
            "SentinelAgent",
            "agent-id.txt");

    private static string? _id;

    public static string Id
    {
        get
        {
            if (!string.IsNullOrWhiteSpace(_id))
            {
                return _id;
            }

            _id = LoadOrCreateId();

            return _id;
        }
    }

    private static string LoadOrCreateId()
    {
        try
        {
            string? directory =
                Path.GetDirectoryName(FilePath);

            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(
                    directory);
            }

            if (File.Exists(FilePath))
            {
                string existing =
                    File.ReadAllText(FilePath)
                        .Trim();

                if (!string.IsNullOrWhiteSpace(existing))
                {
                    return existing;
                }
            }

            string newId =
                Guid.NewGuid().ToString();

            File.WriteAllText(
                FilePath,
                newId);

            return newId;
        }
        catch
        {
            return Guid.NewGuid().ToString();
        }
    }
}
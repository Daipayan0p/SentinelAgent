using System.Collections.Concurrent;
using System.Diagnostics.Eventing.Reader;
using System.IO.Compression;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml;

namespace SentinelAgent;

internal class Program
{
    private static AgentConfig _config = new();

    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(15)
    };

    private static readonly List<EventLogWatcher> Watchers = new();

    private static readonly ConcurrentQueue<WindowsLogEvent> LogQueue = new();

    private static readonly CancellationTokenSource Cancellation = new();

    private static long TotalCollected;
    private static long TotalSent;
    private static long TotalFailed;
    private static long TotalDropped;

    private static int ActiveWatchers;

    // ============================================================
    // MAIN
    // ============================================================

    private static async Task Main()
    {
        // Load configuration before creating a console.
        if (!LoadConfiguration())
        {
            return;
        }

        // WinExe does not create a console automatically.
        // We create one only when runHidden == false.
        if (!_config.RunHidden)
        {
            ConsoleWindow.Allocate();

            Console.Title = "SentinelAgent";

            PrintBanner();
            PrintConfiguration();
        }

        if (!ValidateServerUrl())
        {
            return;
        }

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;

            if (!_config.RunHidden)
            {
                Console.WriteLine();
                Console.WriteLine(
                    "[STOP] Shutdown requested...");
            }

            Cancellation.Cancel();
        };

        StartEventLogCollection();

        if (!_config.RunHidden)
        {
            Console.WriteLine();
            Console.WriteLine(
                $"[READY] Active watchers: {ActiveWatchers}");

            Console.WriteLine(
                "[READY] SentinelAgent is running.");

            Console.WriteLine(
                "[READY] Press CTRL+C to stop.");

            Console.WriteLine();
        }

        try
        {
            await RunSenderLoop(
                Cancellation.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        finally
        {
            StopWatchers();

            using var flushCts =
                new CancellationTokenSource(
                    TimeSpan.FromSeconds(10));

            try
            {
                await SendAvailableLogs(
                    flushCts.Token);
            }
            catch
            {
                // Best effort only.
            }

            if (!_config.RunHidden)
            {
                PrintStatistics();
            }
        }
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
            string configPath =
                Path.Combine(
                    AppContext.BaseDirectory,
                    "config.json");

            if (!File.Exists(configPath))
            {
                CreateDefaultConfiguration(
                    configPath);

                return false;
            }

            string json =
                File.ReadAllText(configPath);

            AgentConfig? config =
                JsonSerializer.Deserialize<AgentConfig>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

            if (config == null)
            {
                return false;
            }

            _config = config;

            if (string.IsNullOrWhiteSpace(
                    _config.ServerUrl))
            {
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

            if (_config.RetryMaxAttempts <= 0)
            {
                _config.RetryMaxAttempts = 3;
            }

            if (_config.RetryBaseDelayMs <= 0)
            {
                _config.RetryBaseDelayMs = 500;
            }

            if (_config.RequestTimeoutSeconds <= 0)
            {
                _config.RequestTimeoutSeconds = 15;
            }

            if (_config.InitialEventsPerChannel < 0)
            {
                _config.InitialEventsPerChannel = 0;
            }

            return true;
        }
        catch
        {
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

            MaxBufferedLogs = 10000,

            RetryMaxAttempts = 3,

            RetryBaseDelayMs = 500,

            RequestTimeoutSeconds = 15,

            CompressPayload = true,

            IncludeRawXml = false,

            CollectExistingEventsOnStartup = false,

            InitialEventsPerChannel = 100,

            RunHidden = false
        };

        string json =
            JsonSerializer.Serialize(
                config,
                new JsonSerializerOptions
                {
                    WriteIndented = true
                });

        File.WriteAllText(
            path,
            json);
    }

    private static void PrintConfiguration()
    {
        Console.WriteLine(
            $"[CONFIG] Server:            {_config.ServerUrl}");

        Console.WriteLine(
            $"[CONFIG] Batch size:        {_config.BatchSize}");

        Console.WriteLine(
            $"[CONFIG] Send interval:     {_config.SendIntervalSeconds}s");

        Console.WriteLine(
            $"[CONFIG] Max buffer:        {_config.MaxBufferedLogs}");

        Console.WriteLine(
            $"[CONFIG] Compression:       {_config.CompressPayload}");

        Console.WriteLine(
            $"[CONFIG] Raw XML:           {_config.IncludeRawXml}");

        Console.WriteLine(
            $"[CONFIG] Historical events: {_config.CollectExistingEventsOnStartup}");

        Console.WriteLine(
            $"[CONFIG] Initial/channel:   {_config.InitialEventsPerChannel}");

        Console.WriteLine(
            $"[CONFIG] Run hidden:        {_config.RunHidden}");

        Console.WriteLine();
    }

    private static bool ValidateServerUrl()
    {
        if (!Uri.TryCreate(
                _config.ServerUrl,
                UriKind.Absolute,
                out Uri? serverUri))
        {
            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    "[ERROR] Invalid serverUrl.");
            }

            return false;
        }

        if (serverUri.Scheme != Uri.UriSchemeHttp &&
            serverUri.Scheme != Uri.UriSchemeHttps)
        {
            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    "[ERROR] serverUrl must use HTTP or HTTPS.");
            }

            return false;
        }

        HttpClient.Timeout =
            TimeSpan.FromSeconds(
                _config.RequestTimeoutSeconds);

        return true;
    }

    // ============================================================
    // EVENT LOG DISCOVERY
    // ============================================================

    private static void StartEventLogCollection()
    {
        if (!_config.RunHidden)
        {
            Console.WriteLine(
                "[INIT] Discovering Windows Event Logs...");
        }

        try
        {
            using EventLogSession session =
                new EventLogSession();

            IEnumerable<string> logNames =
                session.GetLogNames();

            int discovered = 0;

            foreach (string logName in logNames)
            {
                discovered++;

                StartWatcher(logName);

                if (_config.CollectExistingEventsOnStartup)
                {
                    ReadExistingEvents(
                        logName,
                        _config.InitialEventsPerChannel);
                }
            }

            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    $"[INIT] Discovered {discovered} channels.");
            }
        }
        catch (Exception ex)
        {
            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    $"[ERROR] Could not enumerate Event Logs: {ex.Message}");
            }
        }
    }

    // ============================================================
    // EVENT WATCHER
    // ============================================================

    private static void StartWatcher(
        string channel)
    {
        try
        {
            EventLogQuery query =
                new EventLogQuery(
                    channel,
                    PathType.LogName,
                    "*[System[*]]");

            EventLogWatcher watcher =
                new EventLogWatcher(query);

            watcher.EventRecordWritten +=
                OnEventRecordWritten;

            watcher.Enabled = true;

            lock (Watchers)
            {
                Watchers.Add(watcher);
            }

            Interlocked.Increment(
                ref ActiveWatchers);

            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    $"[WATCH] {channel}");
            }
        }
        catch (Exception ex)
        {
            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    $"[SKIP] {channel} - {ex.Message}");
            }
        }
    }

    // ============================================================
    // OPTIONAL HISTORICAL EVENTS
    // ============================================================

    private static void ReadExistingEvents(
        string channel,
        int maximumEvents)
    {
        if (maximumEvents <= 0)
        {
            return;
        }

        try
        {
            EventLogQuery query =
                new EventLogQuery(
                    channel,
                    PathType.LogName,
                    "*[System[*]]");

            using EventLogReader reader =
                new EventLogReader(query);

            var records =
                new List<EventRecord>();

            EventRecord? record;

            while (
                records.Count < maximumEvents &&
                (record = reader.ReadEvent()) != null)
            {
                records.Add(record);
            }

            // Send oldest to newest.
            records.Reverse();

            foreach (EventRecord existingRecord in records)
            {
                using (existingRecord)
                {
                    WindowsLogEvent? log =
                        ConvertEventRecord(
                            existingRecord);

                    if (log == null)
                    {
                        continue;
                    }

                    log.IsHistorical = true;

                    AddToQueue(log);

                    Interlocked.Increment(
                        ref TotalCollected);
                }
            }

            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    $"[INITIAL] {channel}: {records.Count} events");
            }
        }
        catch (Exception ex)
        {
            if (!_config.RunHidden)
            {
                Console.WriteLine(
                    $"[INITIAL-SKIP] {channel}: {ex.Message}");
            }
        }
    }

    // ============================================================
    // LIVE EVENT
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
            using EventRecord record =
                args.EventRecord;

            WindowsLogEvent? log =
                ConvertEventRecord(record);

            if (log == null)
            {
                return;
            }

            AddToQueue(log);

            Interlocked.Increment(
                ref TotalCollected);
        }
        catch
        {
            // Never let one malformed event kill
            // the whole collector.
        }
    }

    // ============================================================
    // EVENT CONVERSION
    // ============================================================

    private static WindowsLogEvent?
        ConvertEventRecord(
            EventRecord record)
    {
        try
        {
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

            string completeXml = string.Empty;
            string rawXml = string.Empty;

            try
            {
                completeXml = record.ToXml();

                if (_config.IncludeRawXml)
                {
                    rawXml = completeXml;
                }
            }
            catch
            {
                // XML extraction failed.
            }

            Dictionary<string, string> eventData =
                ParseEventData(completeXml);

            return new WindowsLogEvent
            {
                AgentId =
                    AgentIdentity.Id,

                Hostname =
                    Environment.MachineName,

                MachineName =
                    record.MachineName ?? string.Empty,

                Channel =
                    record.LogName ?? string.Empty,

                Provider =
                    record.ProviderName ?? string.Empty,

                EventId =
                    record.Id,

                Level =
                    record.LevelDisplayName ?? string.Empty,

                TimeCreated =
                    record.TimeCreated?
                        .ToUniversalTime(),

                RecordId =
                    record.RecordId,

                ProcessId =
                    record.ProcessId,

                ThreadId =
                    record.ThreadId,

                Message =
                    message,

                EventData =
                    eventData,

                RawXml =
                    rawXml,

                IsHistorical = false
            };
        }
        catch
        {
            return null;
        }
    }

    // ============================================================
    // EVENT DATA EXTRACTION
    // ============================================================

    private static Dictionary<string, string>
        ParseEventData(string xml)
    {
        var result =
            new Dictionary<string, string>(
                StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(xml))
        {
            return result;
        }

        try
        {
            var document =
                new XmlDocument();

            document.LoadXml(xml);

            XmlNode? eventData =
                document.SelectSingleNode(
                    "//*[local-name()='EventData']");

            if (eventData == null)
            {
                return result;
            }

            foreach (XmlNode node in eventData.ChildNodes)
            {
                if (!string.Equals(
                        node.LocalName,
                        "Data",
                        StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                string? name =
                    node.Attributes?["Name"]?.Value;

                string value =
                    node.InnerText ?? string.Empty;

                if (!string.IsNullOrWhiteSpace(name))
                {
                    result[name] = value;
                }
            }
        }
        catch
        {
            // Ignore XML parsing failures.
        }

        return result;
    }

    // ============================================================
    // QUEUE
    // ============================================================

    private static void AddToQueue(
        WindowsLogEvent log)
    {
        LogQueue.Enqueue(log);

        while (LogQueue.Count >
               _config.MaxBufferedLogs)
        {
            if (LogQueue.TryDequeue(out _))
            {
                Interlocked.Increment(
                    ref TotalDropped);
            }
            else
            {
                break;
            }
        }
    }

    private static List<WindowsLogEvent>
        GetBatch(int amount)
    {
        var batch =
            new List<WindowsLogEvent>(amount);

        while (
            batch.Count < amount &&
            LogQueue.TryDequeue(
                out WindowsLogEvent? log))
        {
            batch.Add(log);
        }

        return batch;
    }

    private static void ReturnBatch(
        IEnumerable<WindowsLogEvent> logs)
    {
        foreach (WindowsLogEvent log in logs)
        {
            LogQueue.Enqueue(log);
        }

        while (LogQueue.Count >
               _config.MaxBufferedLogs)
        {
            if (LogQueue.TryDequeue(out _))
            {
                Interlocked.Increment(
                    ref TotalDropped);
            }
            else
            {
                break;
            }
        }
    }

    // ============================================================
    // SENDER LOOP
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
                if (!_config.RunHidden)
                {
                    Console.WriteLine(
                        $"[SEND ERROR] {ex.Message}");
                }
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
    // SEND AVAILABLE LOGS
    // ============================================================

    private static async Task SendAvailableLogs(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            List<WindowsLogEvent> batch =
                GetBatch(_config.BatchSize);

            if (batch.Count == 0)
            {
                return;
            }

            var payload =
                new LogBatch
                {
                    AgentId =
                        AgentIdentity.Id,

                    Hostname =
                        Environment.MachineName,

                    OperatingSystem =
                        Environment.OSVersion.VersionString,

                    SentAt =
                        DateTime.UtcNow,

                    Logs =
                        batch
                };

            bool success =
                await SendBatch(
                    payload,
                    cancellationToken);

            if (success)
            {
                Interlocked.Add(
                    ref TotalSent,
                    batch.Count);

                if (!_config.RunHidden)
                {
                    Console.WriteLine(
                        $"[SENT] {batch.Count} logs | " +
                        $"Queue: {LogQueue.Count} | " +
                        $"Total Sent: {TotalSent}");
                }
            }
            else
            {
                Interlocked.Increment(
                    ref TotalFailed);

                ReturnBatch(batch);

                return;
            }
        }
    }

    // ============================================================
    // HTTP SEND + RETRY + GZIP
    // ============================================================

    private static async Task<bool> SendBatch(
        LogBatch payload,
        CancellationToken cancellationToken)
    {
        byte[] jsonBytes =
            JsonSerializer.SerializeToUtf8Bytes(
                payload,
                AgentJsonContext.Default.LogBatch);

        byte[] bodyBytes =
            _config.CompressPayload
                ? Compress(jsonBytes)
                : jsonBytes;

        int attempts =
            Math.Max(
                1,
                _config.RetryMaxAttempts);

        for (
            int attempt = 1;
            attempt <= attempts;
            attempt++)
        {
            try
            {
                using var content =
                    new ByteArrayContent(bodyBytes);

                content.Headers.ContentType =
                    new MediaTypeHeaderValue(
                        "application/json");

                if (_config.CompressPayload)
                {
                    content.Headers.ContentEncoding.Add(
                        "gzip");
                }

                using var request =
                    new HttpRequestMessage(
                        HttpMethod.Post,
                        _config.ServerUrl)
                    {
                        Content = content
                    };

                using HttpResponseMessage response =
                    await HttpClient.SendAsync(
                        request,
                        cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    return true;
                }

                int status =
                    (int)response.StatusCode;

                if (!_config.RunHidden)
                {
                    Console.WriteLine(
                        $"[SERVER] {status} " +
                        $"{response.StatusCode} " +
                        $"(attempt {attempt}/{attempts})");
                }

                // 4xx normally means the request itself
                // is invalid, so retrying is not useful.
                if (status >= 400 &&
                    status < 500)
                {
                    return false;
                }
            }
            catch (
                Exception ex)
                when (
                    ex is HttpRequestException ||
                    ex is TaskCanceledException ||
                    ex is SocketException)
            {
                if (!_config.RunHidden)
                {
                    Console.WriteLine(
                        $"[NETWORK] {ex.Message} " +
                        $"(attempt {attempt}/{attempts})");
                }
            }

            if (attempt < attempts)
            {
                int delay =
                    _config.RetryBaseDelayMs *
                    (int)Math.Pow(
                        2,
                        attempt - 1);

                try
                {
                    await Task.Delay(
                        delay,
                        cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            }
        }

        return false;
    }

    // ============================================================
    // GZIP
    // ============================================================

    private static byte[] Compress(
        byte[] data)
    {
        using var output =
            new MemoryStream();

        using (
            var gzip =
                new GZipStream(
                    output,
                    CompressionLevel.Fastest,
                    leaveOpen: true))
        {
            gzip.Write(
                data,
                0,
                data.Length);
        }

        return output.ToArray();
    }

    // ============================================================
    // STOP WATCHERS
    // ============================================================

    private static void StopWatchers()
    {
        lock (Watchers)
        {
            foreach (EventLogWatcher watcher in Watchers)
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
    }

    // ============================================================
    // STATISTICS
    // ============================================================

    private static void PrintStatistics()
    {
        Console.WriteLine();

        Console.WriteLine(
            "==================================================");

        Console.WriteLine(
            "                 SENTINELAGENT");

        Console.WriteLine(
            "                   STATISTICS");

        Console.WriteLine(
            "==================================================");

        Console.WriteLine(
            $"Collected : {TotalCollected}");

        Console.WriteLine(
            $"Sent      : {TotalSent}");

        Console.WriteLine(
            $"Failed    : {TotalFailed}");

        Console.WriteLine(
            $"Dropped   : {TotalDropped}");

        Console.WriteLine(
            $"Queued    : {LogQueue.Count}");

        Console.WriteLine(
            "==================================================");
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

    [JsonPropertyName("retryMaxAttempts")]
    public int RetryMaxAttempts { get; set; } = 3;

    [JsonPropertyName("retryBaseDelayMs")]
    public int RetryBaseDelayMs { get; set; } = 500;

    [JsonPropertyName("requestTimeoutSeconds")]
    public int RequestTimeoutSeconds { get; set; } = 15;

    [JsonPropertyName("compressPayload")]
    public bool CompressPayload { get; set; } = true;

    [JsonPropertyName("includeRawXml")]
    public bool IncludeRawXml { get; set; } = false;

    [JsonPropertyName("collectExistingEventsOnStartup")]
    public bool CollectExistingEventsOnStartup { get; set; } = false;

    [JsonPropertyName("initialEventsPerChannel")]
    public int InitialEventsPerChannel { get; set; } = 100;

    [JsonPropertyName("runHidden")]
    public bool RunHidden { get; set; } = false;
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
// WINDOWS EVENT
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

    [JsonPropertyName("eventData")]
    public Dictionary<string, string> EventData { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);

    [JsonPropertyName("rawXml")]
    public string RawXml { get; set; } = "";

    [JsonPropertyName("isHistorical")]
    public bool IsHistorical { get; set; }
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

            _id =
                LoadOrCreateId();

            return _id;
        }
    }

    private static string LoadOrCreateId()
    {
        try
        {
            string? directory =
                Path.GetDirectoryName(
                    FilePath);

            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(
                    directory);
            }

            if (File.Exists(FilePath))
            {
                string existing =
                    File.ReadAllText(
                        FilePath)
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


// ================================================================
// SOURCE-GENERATED JSON
// ================================================================

[JsonSourceGenerationOptions(
    PropertyNameCaseInsensitive = true)]

[JsonSerializable(typeof(AgentConfig))]
[JsonSerializable(typeof(LogBatch))]
[JsonSerializable(typeof(WindowsLogEvent))]
[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class AgentJsonContext :
    JsonSerializerContext
{
}


// ================================================================
// WINDOWS CONSOLE CONTROL
// ================================================================

internal static class ConsoleWindow
{
    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool AllocConsole();

    public static void Allocate()
    {
        AllocConsole();
    }
}


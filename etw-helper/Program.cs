using System.Text.Json;
using System.Diagnostics;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Session;

if (TraceEventSession.IsElevated() != true)
{
    Console.Error.WriteLine("ETW kernel FileIO tracing requires administrator rights.");
    return 2;
}

var sessionName = "QbtHddGuardFileIo";
using var session = new TraceEventSession(sessionName) { StopOnDispose = true };
var pidLock = new object();
var qbtPids = new HashSet<int>();
var watchLock = new object();
var watchedRoots = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
long rawReadEvents = 0;
long qbtReadEvents = 0;
long qbtNamedReadEvents = 0;
long otherNamedReadEvents = 0;

RefreshQbtPids();
using var statusTimer = new Timer(_ =>
{
    RefreshQbtPids();
    int[] pids;
    lock (pidLock)
    {
        pids = qbtPids.OrderBy(pid => pid).ToArray();
    }

    Console.Error.WriteLine(
        $"status raw_reads={Interlocked.Read(ref rawReadEvents)} qbt_reads={Interlocked.Read(ref qbtReadEvents)} qbt_named_reads={Interlocked.Read(ref qbtNamedReadEvents)} other_named_reads={Interlocked.Read(ref otherNamedReadEvents)} qbt_pids={string.Join(",", pids)}");
}, null, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    session.Dispose();
};

_ = Task.Run(() =>
{
    while (true)
    {
        var line = Console.In.ReadLine();
        if (line is null)
        {
            return;
        }

        if (line.Equals("quit", StringComparison.OrdinalIgnoreCase) ||
            line.Equals("stop", StringComparison.OrdinalIgnoreCase) ||
            line.Equals("exit", StringComparison.OrdinalIgnoreCase))
        {
            Console.Error.WriteLine("shutdown requested");
            session.Dispose();
            Environment.Exit(0);
        }

        if (line.Equals("clear-watch", StringComparison.OrdinalIgnoreCase))
        {
            lock (watchLock)
            {
                watchedRoots.Clear();
            }
            continue;
        }

        if (line.StartsWith("watch\t", StringComparison.OrdinalIgnoreCase))
        {
            var root = NormalizePath(line.Substring("watch\t".Length));
            if (!string.IsNullOrWhiteSpace(root))
            {
                lock (watchLock)
                {
                    watchedRoots.Add(root);
                }
            }
        }
    }
});

session.EnableKernelProvider(
    KernelTraceEventParser.Keywords.FileIO |
    KernelTraceEventParser.Keywords.FileIOInit |
    KernelTraceEventParser.Keywords.DiskFileIO);

session.Source.Kernel.FileIORead += data =>
{
    Interlocked.Increment(ref rawReadEvents);
    var process = data.ProcessName ?? "";
    var isQbt = IsQbtProcess(data.ProcessID, process);
    if (isQbt)
    {
        Interlocked.Increment(ref qbtReadEvents);
    }
    var path = data.FileName ?? "";
    if (string.IsNullOrWhiteSpace(path))
    {
        return;
    }
    if (!isQbt && !IsWatchedPath(path))
    {
        return;
    }

    if (isQbt)
    {
        Interlocked.Increment(ref qbtNamedReadEvents);
    }
    else
    {
        Interlocked.Increment(ref otherNamedReadEvents);
    }

    var payload = new
    {
        ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0,
        pid = data.ProcessID,
        process = process.EndsWith(".exe", StringComparison.OrdinalIgnoreCase) ? process : process + ".exe",
        is_qbt = isQbt,
        op = "Read",
        path,
        offset = data.Offset,
        size = data.IoSize
    };

    Console.WriteLine(JsonSerializer.Serialize(payload));
    Console.Out.Flush();
};

session.Source.Process();
return 0;

void RefreshQbtPids()
{
    try
    {
        var current = Process.GetProcessesByName("qbittorrent")
            .Select(process => process.Id)
            .ToHashSet();
        lock (pidLock)
        {
            qbtPids.Clear();
            foreach (var pid in current)
            {
                qbtPids.Add(pid);
            }
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"failed to refresh qBittorrent PIDs: {ex.Message}");
    }
}

bool IsQbtProcess(int pid, string processName)
{
    if (processName.Equals("qbittorrent", StringComparison.OrdinalIgnoreCase) ||
        processName.Equals("qbittorrent.exe", StringComparison.OrdinalIgnoreCase))
    {
        return true;
    }

    lock (pidLock)
    {
        return qbtPids.Contains(pid);
    }
}

bool IsWatchedPath(string path)
{
    var normalized = NormalizePath(path);
    lock (watchLock)
    {
        foreach (var root in watchedRoots)
        {
            if (normalized.Equals(root, StringComparison.OrdinalIgnoreCase) ||
                normalized.StartsWith(root + "\\", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }
    }

    return false;
}

string NormalizePath(string path)
{
    if (string.IsNullOrWhiteSpace(path))
    {
        return "";
    }

    try
    {
        return Path.GetFullPath(path.Trim()).TrimEnd('\\', '/');
    }
    catch
    {
        return path.Trim().Replace('/', '\\').TrimEnd('\\');
    }
}

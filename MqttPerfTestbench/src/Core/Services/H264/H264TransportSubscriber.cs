using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.H264;

/// <summary>
/// H.264 Subscriber:
///   TCP Client (framed) → decode pipe → FFmpeg stdout → raw video → Metrics
/// </summary>
public sealed class H264TransportSubscriber : ITransportSubscriber
{
    private readonly PerfMetrics _metrics;
    private TcpClient? _tcpClient;
    private Process?   _decodeProcess;
    private CancellationTokenSource? _cts;

    private long _latestTimestamp = 0;
    private readonly object _tsLock = new();

    public H264TransportSubscriber(PerfMetrics metrics) => _metrics = metrics;

    public async Task ConnectAsync(TransportOptions options)
    {
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        _tcpClient = new TcpClient();
        for (int i = 0; i < 10; i++)
        {
            try
            {
                await _tcpClient.ConnectAsync(options.Server, options.Port, token);
                Console.WriteLine($"[H.264 Sub] Connected to {options.Server}:{options.Port}");
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[H.264 Sub] Attempt {i + 1} failed: {ex.Message}");
                if (i == 9) throw;
                await Task.Delay(1000, token);
            }
        }

        string args =
            $"-fflags nobuffer -flags low_delay " +
            $"-f mpegts -i pipe:0 " +
            $"-f rawvideo -pix_fmt gray pipe:1";

        var psi = new ProcessStartInfo
        {
            FileName               = options.FfmpegPath,
            Arguments              = args,
            UseShellExecute        = false,
            RedirectStandardInput  = true,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            CreateNoWindow         = true
        };

        _decodeProcess = new Process { StartInfo = psi };
        _decodeProcess.ErrorDataReceived += (_, e) => { if (e.Data != null) Console.WriteLine($"[ffmpeg-h264-dec] {e.Data}"); };
        _decodeProcess.Start();
        _decodeProcess.BeginErrorReadLine();

        var decStdin  = _decodeProcess.StandardInput.BaseStream;
        var decStdout = _decodeProcess.StandardOutput.BaseStream;

        // TCP framed connection reader → FFmpeg stdin
        _ = Task.Run(async () =>
        {
            var net = _tcpClient.GetStream();
            var header = new byte[12];
            try
            {
                while (!token.IsCancellationRequested)
                {
                    if (!await ReadExactAsync(net, header, 12, token)) break;
                    int length = BitConverter.ToInt32(header, 0);
                    long ts = BitConverter.ToInt64(header, 4);

                    var buf = new byte[length];
                    if (!await ReadExactAsync(net, buf, length, token)) break;

                    await decStdin.WriteAsync(buf.AsMemory(0, length), token);
                    await decStdin.FlushAsync(token);

                    lock (_tsLock)
                    {
                        _latestTimestamp = ts;
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine($"[H.264 Sub] TCP→FFmpeg: {ex.Message}"); }
            finally { try { decStdin.Close(); } catch { } }
        }, token);

        // FFmpeg decoded raw frame reader → Metrics
        _ = Task.Run(async () =>
        {
            int frameSize = options.Width * options.Height;
            var buf = new byte[frameSize];
            try
            {
                while (!token.IsCancellationRequested)
                {
                    int offset = 0;
                    while (offset < frameSize)
                    {
                        int read = await decStdout.ReadAsync(buf.AsMemory(offset, frameSize - offset), token);
                        if (read == 0) return;
                        offset += read;
                    }

                    long ts;
                    lock (_tsLock)
                    {
                        ts = _latestTimestamp;
                    }

                    if (ts > 0)
                    {
                        long latencyMs = (Stopwatch.GetTimestamp() - ts) * 1000 / Stopwatch.Frequency;
                        _metrics.AddFrame(frameSize, latencyMs);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine($"[H.264 Sub] Decode: {ex.Message}"); }
        }, token);
    }

    private static async Task<bool> ReadExactAsync(NetworkStream stream, byte[] buffer, int count, CancellationToken token)
    {
        int offset = 0;
        while (offset < count)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), token);
            if (read == 0) return false;
            offset += read;
        }
        return true;
    }

    public Task DisconnectAsync()
    {
        try { _cts?.Cancel(); } catch { }
        try { _cts?.Dispose(); } catch { }
        _cts = null;
        try { if (_decodeProcess is { HasExited: false }) _decodeProcess.Kill(true); } catch { }
        _decodeProcess?.Dispose(); _decodeProcess = null;
        try { _tcpClient?.Close(); } catch { }
        _tcpClient = null;
        return Task.CompletedTask;
    }

    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();
}

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.H264;

/// <summary>
/// H.264 Publisher:
///   rawPayload → FFmpeg stdin (encoding) → FFmpeg stdout → TCP Socket (framed) → Client
/// Uses reliable TCP framing to transmit precise timestamps outside the lossy video stream.
/// </summary>
public sealed class H264TransportPublisher : ITransportPublisher
{
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    private Process?     _encodeProcess;
    private Stream?      _ffmpegStdin;
    private Stream?      _ffmpegStdout;

    private TcpListener? _listener;
    private volatile TcpClient?     _client;
    private volatile NetworkStream? _clientStream;

    private CancellationTokenSource? _openCts;
    private CancellationTokenSource? _publishCts;

    public async Task OpenAsync(TransportOptions options)
    {
        _openCts = new CancellationTokenSource();
        var token = _openCts.Token;

        StartEncoder(options);
        await KickstartAsync(options);

        // FFmpeg stdout drain loop with robust TCP framing
        _ = Task.Run(async () =>
        {
            var buf = new byte[65536];
            var header = new byte[12]; // [4 bytes length][8 bytes timestamp]
            try
            {
                while (!token.IsCancellationRequested && _ffmpegStdout != null)
                {
                    int read = await _ffmpegStdout.ReadAsync(buf, token);
                    if (read == 0) break;

                    var stream = _clientStream;
                    if (stream != null)
                    {
                        try
                        {
                            BitConverter.TryWriteBytes(header.AsSpan(0, 4), read);
                            BitConverter.TryWriteBytes(header.AsSpan(4, 8), Stopwatch.GetTimestamp());
                            await stream.WriteAsync(header, token);
                            await stream.WriteAsync(buf.AsMemory(0, read), token);
                        }
                        catch
                        {
                            // Client disconnected, handled in accept loop
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine($"[H.264] Drain error: {ex.Message}"); }
        }, token);

        _listener = new TcpListener(IPAddress.Any, options.Port);
        _listener.Start();
        Console.WriteLine($"[H.264 Publisher] Listening on port {options.Port}");

        _ = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var newClient = await _listener.AcceptTcpClientAsync(token);
                    var ep = newClient.Client.RemoteEndPoint?.ToString() ?? "unknown";

                    var old = _client;
                    if (old != null)
                    {
                        var oldEp = "unknown";
                        try { oldEp = old.Client.RemoteEndPoint?.ToString() ?? "unknown"; } catch { }
                        _clientStream = null;
                        old.Close();
                        ClientDisconnected?.Invoke(oldEp);
                    }

                    _client = newClient;
                    _clientStream = newClient.GetStream();
                    Console.WriteLine($"[H.264] Client connected: {ep}");
                    ClientConnected?.Invoke(ep);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    if (!token.IsCancellationRequested)
                        Console.WriteLine($"[H.264] Accept error: {ex.Message}");
                }
            }
        }, token);
    }

    public Task CloseAsync()
    {
        _publishCts?.Cancel();
        _openCts?.Cancel();
        _listener?.Stop();
        _clientStream = null;
        _client?.Close();
        KillEncoder();
        _openCts?.Dispose();
        _publishCts?.Dispose();
        _openCts = null;
        _publishCts = null;
        return Task.CompletedTask;
    }

    public void StartPublishing(byte[] payload, int targetFps, TransportOptions options)
    {
        if (_ffmpegStdin == null) return;
        StopPublishing();

        _publishCts = new CancellationTokenSource();
        var token = _publishCts.Token;

        Task.Run(async () =>
        {
            double msPerFrame = 1000.0 / targetFps;
            var sw = new Stopwatch();
            try
            {
                while (!token.IsCancellationRequested && _ffmpegStdin != null)
                {
                    sw.Restart();
                    await _ffmpegStdin.WriteAsync(payload, token);
                    await _ffmpegStdin.FlushAsync(token);
                    int delay = (int)(msPerFrame - sw.ElapsedMilliseconds);
                    if (delay > 0) await Task.Delay(delay, token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine($"[H.264] Publish loop error: {ex.Message}"); }
        }, token);
    }

    public void StopPublishing()
    {
        _publishCts?.Cancel();
        _publishCts?.Dispose();
        _publishCts = null;
    }

    public void Dispose() => CloseAsync().GetAwaiter().GetResult();

    private void StartEncoder(TransportOptions options)
    {
        string codec   = GetCodec(options);
        string quality = GetQualityArgs(options, codec);
        int fps = 30;

        string args =
            $"-y -fflags nobuffer -flags low_delay " +
            $"-f rawvideo -pix_fmt gray -s {options.Width}x{options.Height} -r {fps} -i pipe:0 " +
            $"-an -c:v {codec} {quality} -pix_fmt yuv420p " +
            $"-f mpegts -flush_packets 1 pipe:1";

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

        _encodeProcess = new Process { StartInfo = psi };
        _encodeProcess.ErrorDataReceived += (_, e) =>
        {
            if (e.Data != null) Console.WriteLine($"[ffmpeg-h264-enc] {e.Data}");
        };
        _encodeProcess.Start();
        _encodeProcess.BeginErrorReadLine();

        _ffmpegStdin  = _encodeProcess.StandardInput.BaseStream;
        _ffmpegStdout = _encodeProcess.StandardOutput.BaseStream;
        Console.WriteLine($"[H.264] Encoder started (codec={codec})");
    }

    private async Task KickstartAsync(TransportOptions options)
    {
        if (_ffmpegStdin == null) return;
        long frameSize = (long)options.Width * options.Height;
        byte[] chunk = new byte[1 << 20];
        long sent = 0;
        while (sent < frameSize)
        {
            int toSend = (int)Math.Min(chunk.Length, frameSize - sent);
            await _ffmpegStdin.WriteAsync(chunk.AsMemory(0, toSend));
            sent += toSend;
        }
        await _ffmpegStdin.FlushAsync();
        Console.WriteLine($"[H.264] Kickstart done ({frameSize} bytes)");
    }

    private void KillEncoder()
    {
        try { _ffmpegStdin?.Close(); } catch { }
        try { if (_encodeProcess is { HasExited: false }) _encodeProcess.Kill(true); } catch { }
        _encodeProcess?.Dispose();
        _encodeProcess = null;
        _ffmpegStdin   = null;
        _ffmpegStdout  = null;
    }

    private static string GetCodec(TransportOptions o)
    {
        if (o.UseGpu)
        {
            if (OperatingSystem.IsMacOS()) return "h264_videotoolbox";
            return "h264_nvenc";
        }
        return "libx264";
    }

    private static string GetQualityArgs(TransportOptions o, string codec) => codec switch
    {
        "h264_videotoolbox" => "-realtime true -q:v 50 -g 1",
        "h264_nvenc"        => $"-preset p1 -tune ll -rc vbr -cq {o.Crf} -bf 0 -g 1",
        _                   => $"-preset ultrafast -tune zerolatency -crf {o.Crf} -g 1"
    };
}

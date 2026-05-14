using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Models;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.H265;

public class H265TransportSubscriber : ITransportSubscriber
{
    private Process? _ffmpegProcess;
    private readonly PerfMetrics _metrics;
    private CancellationTokenSource? _cts;

    public H265TransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public Task ConnectAsync(TransportOptions options)
    {
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        string inputUrl = $"tcp://{options.Server}:{options.Port}";
        string args = $"-i \"{inputUrl}\" -f rawvideo -pix_fmt bgra pipe:1";

        var psi = new ProcessStartInfo
        {
            FileName = options.FfmpegPath,
            Arguments = args,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        try
        {
            _ffmpegProcess = new Process { StartInfo = psi };
            _ffmpegProcess.ErrorDataReceived += (s, e) => { if (e.Data != null) Console.WriteLine($"[ffmpeg-sub] {e.Data}"); };
            _ffmpegProcess.Start();
            _ffmpegProcess.BeginErrorReadLine();
            var stdout = _ffmpegProcess.StandardOutput.BaseStream;
            Console.WriteLine($"FFmpeg Subscriber connecting to {inputUrl}");

            Task.Run(async () =>
            {
                int frameSize = options.Width * options.Height * 4;
                byte[] buffer = new byte[frameSize];

                try
                {
                    while (!token.IsCancellationRequested && stdout != null)
                    {
                        int offset = 0;
                        while (offset < frameSize)
                        {
                            int read = await stdout.ReadAsync(buffer.AsMemory(offset, frameSize - offset), token);
                            if (read == 0) break;
                            offset += read;
                        }

                        if (offset == frameSize)
                        {
                            long startTimestamp = BitConverter.ToInt64(buffer, 0);
                            long latency = (Stopwatch.GetTimestamp() - startTimestamp) * 1000 / Stopwatch.Frequency;
                            _metrics.AddFrame(frameSize, latency);
                        }
                    }
                }
                catch { /* Ignore */ }
            }, token);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Failed to start FFmpeg Subscriber: {ex.Message}");
        }

        return Task.CompletedTask;
    }

    public async Task DisconnectAsync()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;

        try
        {
            if (_ffmpegProcess != null && !_ffmpegProcess.HasExited)
            {
                _ffmpegProcess.Kill(true);
            }
        }
        catch { }

        _ffmpegProcess?.Dispose();
        _ffmpegProcess = null;
        await Task.CompletedTask;
    }
}

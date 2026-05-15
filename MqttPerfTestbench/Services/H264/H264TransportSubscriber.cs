using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Models;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.H264;

public class H264TransportSubscriber : ITransportSubscriber
{
    private Process? _ffmpegProcess;
    private readonly PerfMetrics _metrics;
    private CancellationTokenSource? _cts;

    public H264TransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public async Task ConnectAsync(TransportOptions options)
    {
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        int port = options.Port == 9000 ? 9001 : options.Port;
        string inputUrl = $"tcp://127.0.0.1:{port}";
        
        for (int i = 0; i < 5; i++)
        {
            try
            {
                // Use pix_fmt gray for grayscale output
                string args = $"-fflags nobuffer -flags low_delay -i \"{inputUrl}\" -f rawvideo -pix_fmt gray pipe:1";

                var psi = new ProcessStartInfo
                {
                    FileName = options.FfmpegPath,
                    Arguments = args,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };

                _ffmpegProcess = new Process { StartInfo = psi };
                _ffmpegProcess.ErrorDataReceived += (s, e) => { if (e.Data != null) Console.WriteLine($"[h264-sub] {e.Data}"); };
                _ffmpegProcess.Start();
                _ffmpegProcess.BeginErrorReadLine();
                var stdout = _ffmpegProcess.StandardOutput.BaseStream;
                
                Console.WriteLine($"H.264 Subscriber connected to {inputUrl}");

                Task.Run(async () =>
                {
                    int frameSize = options.Width * options.Height;
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
                    catch (Exception ex)
                    {
                        Console.WriteLine($"H.264 Subscriber loop error: {ex.Message}");
                    }
                }, token);

                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"H.264 Subscriber connect attempt {i+1} failed: {ex.Message}");
                await Task.Delay(1000, token);
            }
        }
    }

    public async Task DisconnectAsync()
    {
        _cts?.Cancel();
        try { if (_ffmpegProcess != null && !_ffmpegProcess.HasExited) _ffmpegProcess.Kill(true); } catch { }
        _ffmpegProcess?.Dispose();
        _ffmpegProcess = null;
        await Task.CompletedTask;
    }
}

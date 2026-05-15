using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.H264;

public class H264TransportPublisher : ITransportPublisher
{
    private Process? _ffmpegProcess;
    private Stream? _ffmpegStdin;
    private CancellationTokenSource? _cts;
    private TransportOptions? _options;

    public async Task ConnectAsync(TransportOptions options)
    {
        _options = options;
        _cts = new CancellationTokenSource();
        
        StartFfmpeg(options, 30); 
        
        if (_ffmpegStdin != null)
        {
            try
            {
                // Grayscale Kickstart: Width * Height * 1
                long frameSize = (long)options.Width * options.Height;
                byte[] chunk = new byte[1024 * 1024];
                
                long sent = 0;
                while (sent < frameSize)
                {
                    int toSend = (int)Math.Min(chunk.Length, frameSize - sent);
                    await _ffmpegStdin.WriteAsync(chunk, 0, toSend);
                    sent += toSend;
                }
                await _ffmpegStdin.FlushAsync();
                Console.WriteLine($"H.264 Publisher kickstarted with {frameSize} bytes grayscale frame.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"H.264 Kickstart failed: {ex.Message}");
            }
        }
    }

    public Task DisconnectAsync()
    {
        StopPublishing();
        return Task.CompletedTask;
    }

    private void StartFfmpeg(TransportOptions options, int targetFps)
    {
        if (_ffmpegProcess != null && !_ffmpegProcess.HasExited) return;

        string codec = "libx264";
        string qualityParams = $"-preset ultrafast -tune zerolatency -crf {options.Crf}";

        if (OperatingSystem.IsMacOS())
        {
            codec = "h264_videotoolbox"; // Mac hardware H.264
            qualityParams = "-realtime true -q:v 50";
        }
        else if (options.UseGpu)
        {
            codec = "h264_nvenc";
            qualityParams = $"-preset p1 -tune ll -rc vbr -cq {options.Crf} -bf 0 -g {targetFps}";
        }

        // Port 9001 for H.264 to avoid conflict with H.265(9000)
        int port = options.Port == 9000 ? 9001 : options.Port;
        string outputUrl = $"tcp://0.0.0.0:{port}?listen=1";
        
        // Use -pix_fmt gray for 1-byte-per-pixel grayscale source (64MB for 8K)
        string args = $"-y -fflags nobuffer -flags low_delay -f rawvideo -pix_fmt gray -s {options.Width}x{options.Height} -r {targetFps} -i pipe:0 " +
                      $"-an -c:v {codec} {qualityParams} -flush_packets 1 -f matroska \"{outputUrl}\"";

        try
        {
            _ffmpegProcess = Process.Start(new ProcessStartInfo {
                FileName = options.FfmpegPath, Arguments = args, UseShellExecute = false, 
                RedirectStandardInput = true, RedirectStandardError = true, CreateNoWindow = true
            });
            _ffmpegProcess!.ErrorDataReceived += (s, e) => { if (e.Data != null) Console.WriteLine($"[h264-pub] {e.Data}"); };
            _ffmpegProcess.BeginErrorReadLine();
            _ffmpegStdin = _ffmpegProcess.StandardInput.BaseStream;
            Console.WriteLine($"H.264 Publisher started on {outputUrl}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to start H.264 FFmpeg: {ex.Message}");
        }
    }

    public void StartPublishing(byte[] rawPayload, int targetFps, TransportOptions options)
    {
        var token = _cts?.Token ?? CancellationToken.None;
        Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested && _ffmpegStdin != null)
                {
                    var sw = Stopwatch.StartNew();
                    BitConverter.TryWriteBytes(rawPayload.AsSpan(0, 8), Stopwatch.GetTimestamp());
                    await _ffmpegStdin.WriteAsync(rawPayload, token);
                    int delay = (int)(1000.0 / targetFps - sw.ElapsedMilliseconds);
                    if (delay > 0) await Task.Delay(delay, token);
                }
            }
            catch { }
        }, token);
    }

    public void StopPublishing()
    {
        _cts?.Cancel();
        try { if (_ffmpegProcess != null && !_ffmpegProcess.HasExited) _ffmpegProcess.Kill(true); } catch { }
        _ffmpegProcess?.Dispose();
        _ffmpegProcess = null;
        _ffmpegStdin = null;
    }
}

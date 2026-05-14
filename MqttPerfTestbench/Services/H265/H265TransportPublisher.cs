using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.H265;

public class H265TransportPublisher : ITransportPublisher
{
    private Process? _ffmpegProcess;
    private Stream? _ffmpegStdin;
    private CancellationTokenSource? _cts;

    public Task ConnectAsync(TransportOptions options)
    {
        return Task.CompletedTask;
    }

    public Task DisconnectAsync()
    {
        StopPublishing();
        return Task.CompletedTask;
    }

    public void StartPublishing(byte[] rawPayload, int targetFps, TransportOptions options)
    {
        StopPublishing();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        // OS optimized commands
        string codec = "libx265";
        string qualityParams = $"-preset ultrafast -tune zerolatency -crf {options.Crf} -x265-params \"keyint={targetFps}:min-keyint={targetFps}:scenecut=0:bframes=0:rc-lookahead=0\"";

        if (OperatingSystem.IsMacOS())
        {
            // Mac VideoToolbox (Hardware Acceleration)
            codec = "hevc_videotoolbox";
            // -realtime true and -q:v 50 for good balance on Mac
            qualityParams = $"-realtime true -q:v 50 -allow_sw true"; 
        }
        else if (options.UseGpu)
        {
            // Windows/Linux NVIDIA NVENC
            codec = "hevc_nvenc";
            qualityParams = $"-preset p1 -tune ll -rc vbr -cq {options.Crf} -bf 0 -g {targetFps}";
        }

        string outputUrl = $"tcp://127.0.0.1:{options.Port}?listen=1";
        
        string args = $"-y -f rawvideo -pix_fmt bgra -s {options.Width}x{options.Height} -r {targetFps} -i pipe:0 " +
                      $"-an -c:v {codec} {qualityParams} -pix_fmt yuv420p -f matroska \"{outputUrl}\"";

        var psi = new ProcessStartInfo
        {
            FileName = options.FfmpegPath,
            Arguments = args,
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        try
        {
            _ffmpegProcess = new Process { StartInfo = psi };
            _ffmpegProcess.ErrorDataReceived += (s, e) => { if (e.Data != null) Console.WriteLine($"[ffmpeg-pub] {e.Data}"); };
            _ffmpegProcess.Start();
            _ffmpegProcess.BeginErrorReadLine();
            _ffmpegStdin = _ffmpegProcess.StandardInput.BaseStream;
            Console.WriteLine($"FFmpeg Publisher started with codec {codec} on {outputUrl}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to start FFmpeg at {options.FfmpegPath}: {ex.Message}");
            return;
        }

        Task.Run(async () =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var sw = Stopwatch.StartNew();
                    
                    // Inject timestamp for latency measurement
                    BitConverter.TryWriteBytes(rawPayload.AsSpan(0, 8), Stopwatch.GetTimestamp());

                    if (_ffmpegStdin != null)
                    {
                        await _ffmpegStdin.WriteAsync(rawPayload, token);
                        await _ffmpegStdin.FlushAsync(token);
                    }

                    int delay = (int)(1000.0 / targetFps - sw.ElapsedMilliseconds);
                    if (delay > 0) await Task.Delay(delay, token);
                }
            }
            catch { /* Ignore */ }
        }, token);
    }

    public void StopPublishing()
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
        _ffmpegStdin = null;
    }
}

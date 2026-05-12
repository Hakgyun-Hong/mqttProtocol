using System;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.Zmq;

public class ZmqTransportPublisher : ITransportPublisher
{
    private PublisherSocket? _pubSocket;
    private CancellationTokenSource? _cts;

    public Task ConnectAsync(TransportOptions options)
    {
        _pubSocket = new PublisherSocket();
        _pubSocket.Options.SendHighWatermark = options.HighWatermark;
        
        // ZMQ doesn't have an explicit NoDelay option exposed like standard sockets, 
        // but batching can be disabled by setting Linger to 0.
        _pubSocket.Options.Linger = TimeSpan.Zero;
        
        _pubSocket.Bind($"tcp://{options.Server}:{options.Port}");
        return Task.CompletedTask;
    }

    public Task DisconnectAsync()
    {
        StopPublishing();
        if (_pubSocket != null)
        {
            _pubSocket.Dispose();
            _pubSocket = null;
        }
        return Task.CompletedTask;
    }

    public void StartPublishing(byte[] compressedPayload, int targetFps, TransportOptions options)
    {
        if (_pubSocket == null) throw new InvalidOperationException("ZMQ Socket not bound.");

        StopPublishing();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(async () =>
        {
            double targetFrameTimeMs = 1000.0 / targetFps;
            var sw = new System.Diagnostics.Stopwatch();
            long frameId = 0;

            while (!token.IsCancellationRequested)
            {
                sw.Restart();
                
                // Embed Timestamp (8 bytes) at start for latency test
                long currentTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
                BitConverter.TryWriteBytes(compressedPayload.AsSpan(0, 8), currentTimestamp);
                BitConverter.TryWriteBytes(compressedPayload.AsSpan(8, 8), frameId);

                // Send frame
                _pubSocket.SendFrame(compressedPayload);
                frameId++;

                sw.Stop();
                double elapsedMs = sw.Elapsed.TotalMilliseconds;
                if (elapsedMs < targetFrameTimeMs)
                {
                    int delayMs = (int)(targetFrameTimeMs - elapsedMs);
                    if (delayMs > 0)
                    {
                        await Task.Delay(delayMs, token);
                    }
                }
            }
        }, token);
    }

    public void StopPublishing()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;
    }
}

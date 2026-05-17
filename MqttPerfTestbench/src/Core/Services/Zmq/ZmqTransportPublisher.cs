using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Zmq;

public sealed class ZmqTransportPublisher : ITransportPublisher
{
    // ZMQ PUB-SUB은 연결 감지 어려움 → 이벤트 미구현
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    private PublisherSocket? _pubSocket;
    private CancellationTokenSource? _cts;

    public Task OpenAsync(TransportOptions options)
    {
        _pubSocket = new PublisherSocket();
        _pubSocket.Options.SendHighWatermark = options.HighWatermark;
        _pubSocket.Options.Linger = TimeSpan.Zero;
        _pubSocket.Bind($"tcp://0.0.0.0:{options.Port}");
        Console.WriteLine($"[ZMQ Publisher] Bound on port {options.Port}");
        return Task.CompletedTask;
    }

    public Task CloseAsync()
    {
        StopPublishing();
        _pubSocket?.Dispose();
        _pubSocket = null;
        return Task.CompletedTask;
    }

    public void StartPublishing(byte[] payload, int targetFps, TransportOptions options)
    {
        if (_pubSocket == null) return;
        StopPublishing();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(async () =>
        {
            double msPerFrame = 1000.0 / targetFps;
            var sw = new Stopwatch();
            long frameId = 0;
            while (!token.IsCancellationRequested)
            {
                sw.Restart();
                BitConverter.TryWriteBytes(payload.AsSpan(0, 8), Stopwatch.GetTimestamp());
                BitConverter.TryWriteBytes(payload.AsSpan(8, 8), frameId++);
                _pubSocket.SendFrame(payload);
                int delay = (int)(msPerFrame - sw.ElapsedMilliseconds);
                if (delay > 0) await Task.Delay(delay, token);
            }
        }, token);
    }

    public void StopPublishing()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;
    }

    public void Dispose() => CloseAsync().GetAwaiter().GetResult();
}

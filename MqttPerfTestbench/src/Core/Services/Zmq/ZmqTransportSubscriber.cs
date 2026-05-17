using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Zmq;

public sealed class ZmqTransportSubscriber : ITransportSubscriber
{
    private readonly PerfMetrics _metrics;
    private SubscriberSocket? _subSocket;
    private CancellationTokenSource? _cts;

    public ZmqTransportSubscriber(PerfMetrics metrics) => _metrics = metrics;

    public Task ConnectAsync(TransportOptions options)
    {
        _subSocket = new SubscriberSocket();
        _subSocket.Options.ReceiveHighWatermark = options.HighWatermark;
        _subSocket.Connect($"tcp://{options.Server}:{options.Port}");
        _subSocket.SubscribeToAnyTopic();
        Console.WriteLine($"[ZMQ Sub] Connected to {options.Server}:{options.Port}");

        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(() =>
        {
            while (!token.IsCancellationRequested)
            {
                if (_subSocket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(100), out var payload)
                    && payload is { Length: >= 16 })
                {
                    long ts = BitConverter.ToInt64(payload, 0);
                    long latencyMs = (long)((Stopwatch.GetTimestamp() - ts) * 1000.0 / Stopwatch.Frequency);
                    _metrics.AddFrame(payload.Length, latencyMs);
                }
            }
        }, token);

        return Task.CompletedTask;
    }

    public Task DisconnectAsync()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _subSocket?.Dispose();
        _subSocket = null;
        return Task.CompletedTask;
    }

    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();
}

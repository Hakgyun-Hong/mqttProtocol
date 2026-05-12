using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;
using MqttPerfTestbench.Models.Interfaces;
using MqttPerfTestbench.Models;

namespace MqttPerfTestbench.Services.Zmq;

public class ZmqTransportSubscriber : ITransportSubscriber
{
    private SubscriberSocket? _subSocket;
    private readonly PerfMetrics _metrics;
    private CancellationTokenSource? _cts;

    public ZmqTransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public Task ConnectAsync(TransportOptions options)
    {
        _subSocket = new SubscriberSocket();
        _subSocket.Options.ReceiveHighWatermark = options.HighWatermark;
        _subSocket.Connect($"tcp://{options.Server}:{options.Port}");
        _subSocket.SubscribeToAnyTopic();

        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(() =>
        {
            while (!token.IsCancellationRequested)
            {
                if (_subSocket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(100), out var payload))
                {
                    if (payload != null && payload.Length >= 16)
                    {
                        long startTimestamp = BitConverter.ToInt64(payload, 0);
                        long currentTimestamp = Stopwatch.GetTimestamp();
                        double latencyMs = (currentTimestamp - startTimestamp) * 1000.0 / Stopwatch.Frequency;
                        
                        _metrics.AddFrame(payload.Length, (long)latencyMs);
                    }
                }
            }
        }, token);

        return Task.CompletedTask;
    }

    public Task DisconnectAsync()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        if (_subSocket != null)
        {
            _subSocket.Dispose();
            _subSocket = null;
        }
        return Task.CompletedTask;
    }
}

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using ProtoBuf.Grpc.Client;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Grpc;

public sealed class GrpcTransportSubscriber : ITransportSubscriber
{
    private readonly PerfMetrics _metrics;
    private GrpcChannel? _channel;
    private CancellationTokenSource? _cts;

    public GrpcTransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public async Task ConnectAsync(TransportOptions options)
    {
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        var channelOptions = new GrpcChannelOptions
        {
            MaxReceiveMessageSize = 100 * 1024 * 1024,
            MaxSendMessageSize = 100 * 1024 * 1024
        };

        string address = $"http://{options.Server}:{options.Port}";
        _channel = GrpcChannel.ForAddress(address, channelOptions);
        var client = _channel.CreateGrpcService<IGrpcImageService>();

        Console.WriteLine($"[gRPC Sub] Connecting to {address}...");

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var frame in client.SubscribeFramesAsync(token))
                {
                    if (frame.Payload is { Length: >= 16 })
                    {
                        long ts = BitConverter.ToInt64(frame.Payload, 0);
                        long latencyMs = (long)((Stopwatch.GetTimestamp() - ts) * 1000.0 / Stopwatch.Frequency);
                        _metrics.AddFrame(frame.Payload.Length, latencyMs);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Console.WriteLine($"[gRPC Sub] Stream error: {ex.Message}");
            }
        }, token);

        await Task.CompletedTask;
    }

    public Task DisconnectAsync()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;

        if (_channel != null)
        {
            _channel.Dispose();
            _channel = null;
        }

        return Task.CompletedTask;
    }

    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();
}

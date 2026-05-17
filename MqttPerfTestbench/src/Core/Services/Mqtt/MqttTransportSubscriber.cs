using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Mqtt;

public sealed class MqttTransportSubscriber : ITransportSubscriber
{
    private readonly PerfMetrics _metrics;
    private IMqttClient? _mqttClient;

    public MqttTransportSubscriber(PerfMetrics metrics) => _metrics = metrics;

    public async Task ConnectAsync(TransportOptions options)
    {
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        var clientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(o =>
            {
                o.Server  = options.Server;
                o.Port    = options.Port;
                o.NoDelay = options.TcpNoDelay;
            })
            .Build();

        _mqttClient.ApplicationMessageReceivedAsync += e =>
        {
            var seg = e.ApplicationMessage.PayloadSegment;
            if (seg.Count >= 16)
            {
                long ts = BitConverter.ToInt64(seg.Array!, seg.Offset);
                long latencyMs = (long)((Stopwatch.GetTimestamp() - ts) * 1000.0 / Stopwatch.Frequency);
                _metrics.AddFrame(seg.Count, latencyMs);
            }
            return Task.CompletedTask;
        };

        await _mqttClient.ConnectAsync(clientOptions);

        var subscribeOptions = factory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("bench/chunk/+"))
            .Build();
        await _mqttClient.SubscribeAsync(subscribeOptions);
        Console.WriteLine($"[MQTT Sub] Connected to {options.Server}:{options.Port}");
    }

    public async Task DisconnectAsync()
    {
        if (_mqttClient != null)
        {
            await _mqttClient.DisconnectAsync();
            _mqttClient.Dispose();
            _mqttClient = null;
        }
    }

    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();
}

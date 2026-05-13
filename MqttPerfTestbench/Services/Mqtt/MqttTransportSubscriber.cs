using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MqttPerfTestbench.Models;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.Mqtt;

public class MqttTransportSubscriber : ITransportSubscriber
{
    private IMqttClient? _mqttClient;
    private readonly PerfMetrics _metrics;

    public MqttTransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public async Task ConnectAsync(TransportOptions options)
    {
        var mqttFactory = new MqttFactory();
        _mqttClient = mqttFactory.CreateMqttClient();

        var mqttClientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(o =>
            {
                o.Server = options.Server;
                o.Port = options.Port;
                o.NoDelay = options.TcpNoDelay;
                // Cap buffer size for Mac/Linux if needed, or rely on defaults
                // o.BufferSize = options.BufferSizeMb * 1024 * 1024; 
            })
            .Build();

        _mqttClient.ApplicationMessageReceivedAsync += e =>
        {
            var payload = e.ApplicationMessage.Payload;
            if (payload != null && payload.Length >= 16)
            {
                long startTimestamp = BitConverter.ToInt64(payload, 0);
                long currentTimestamp = Stopwatch.GetTimestamp();
                double latencyMs = (currentTimestamp - startTimestamp) * 1000.0 / Stopwatch.Frequency;
                
                _metrics.AddFrame(payload.Length, (long)latencyMs);
            }
            return Task.CompletedTask;
        };

        await _mqttClient.ConnectAsync(mqttClientOptions);

        var mqttSubscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("testbench/image/full"))
            .WithTopicFilter(f => f.WithTopic("testbench/image/chunk/+"))
            .Build();

        await _mqttClient.SubscribeAsync(mqttSubscribeOptions, CancellationToken.None);
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
}

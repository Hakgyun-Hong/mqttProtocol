using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MqttPerfTestbench.Models;

namespace MqttPerfTestbench.Services;

public class MqttSubscriberService
{
    private IMqttClient? _mqttClient;
    private readonly PerfMetrics _metrics;

    public MqttSubscriberService(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public async Task ConnectAsync(string server, int port, bool tcpNoDelay = true, int bufferSize = 16 * 1024 * 1024)
    {
        var mqttFactory = new MqttFactory();
        _mqttClient = mqttFactory.CreateMqttClient();

        var mqttClientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(o =>
            {
                o.Server = server;
                o.Port = port;
                o.NoDelay = tcpNoDelay;
                o.BufferSize = bufferSize;
            })
            .Build();

        _mqttClient.ApplicationMessageReceivedAsync += e =>
        {
            var payload = e.ApplicationMessage.Payload;
            if (payload != null && payload.Length >= 16)
            {
                // Read timestamp (8 bytes)
                long startTimestamp = BitConverter.ToInt64(payload, 0);
                long currentTimestamp = Stopwatch.GetTimestamp();
                
                // Calculate latency in ms
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

    public void ResetMetrics()
    {
        _metrics.Reset();
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

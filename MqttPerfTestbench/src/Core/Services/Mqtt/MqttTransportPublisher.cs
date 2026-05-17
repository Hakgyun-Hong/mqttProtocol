using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Mqtt;

public sealed class MqttTransportPublisher : ITransportPublisher
{
    // MQTT Broker의 ClientConnected 이벤트를 MqttBrokerService에서 구독해서 사용
    // Publisher 자체에서는 연결 감지 불가 → 미구현
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    private IMqttClient? _mqttClient;
    private CancellationTokenSource? _cts;

    public async Task OpenAsync(TransportOptions options)
    {
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        var clientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(o =>
            {
                o.Server  = options.Server == "0.0.0.0" ? "127.0.0.1" : options.Server;
                o.Port    = options.Port;
                o.NoDelay = options.TcpNoDelay;
            })
            .Build();

        await _mqttClient.ConnectAsync(clientOptions);
        Console.WriteLine($"[MQTT Publisher] Connected to broker {options.Server}:{options.Port}");
    }

    public async Task CloseAsync()
    {
        StopPublishing();
        if (_mqttClient != null)
        {
            await _mqttClient.DisconnectAsync();
            _mqttClient.Dispose();
            _mqttClient = null;
        }
    }

    public void StartPublishing(byte[] payload, int targetFps, TransportOptions options)
    {
        if (_mqttClient == null || !_mqttClient.IsConnected) return;
        StopPublishing();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(async () =>
        {
            double msPerFrame = 1000.0 / targetFps;
            var sw = new Stopwatch();
            long frameId = 0;

            while (!token.IsCancellationRequested && _mqttClient.IsConnected)
            {
                sw.Restart();
                BitConverter.TryWriteBytes(payload.AsSpan(0, 8), Stopwatch.GetTimestamp());
                BitConverter.TryWriteBytes(payload.AsSpan(8, 8), frameId++);

                int chunkBytes = options.ChunkSizeMb * 1024 * 1024;
                int chunks = (int)Math.Ceiling((double)payload.Length / chunkBytes);

                for (int i = 0; i < chunks; i++)
                {
                    int offset = i * chunkBytes;
                    int length = Math.Min(chunkBytes, payload.Length - offset);
                    var msg = new MqttApplicationMessageBuilder()
                        .WithTopic($"bench/chunk/{i}")
                        .WithPayload(new ArraySegment<byte>(payload, offset, length))
                        .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)options.Qos)
                        .Build();
                    await _mqttClient.PublishAsync(msg, token);
                }

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

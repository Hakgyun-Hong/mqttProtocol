using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.Mqtt;

public class MqttTransportPublisher : ITransportPublisher
{
    private IMqttClient? _mqttClient;
    private CancellationTokenSource? _cts;

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
                // o.BufferSize = options.BufferSizeMb * 1024 * 1024;
            })
            .Build();

        await _mqttClient.ConnectAsync(mqttClientOptions);
    }

    public async Task DisconnectAsync()
    {
        StopPublishing();
        if (_mqttClient != null)
        {
            await _mqttClient.DisconnectAsync();
            _mqttClient.Dispose();
            _mqttClient = null;
        }
    }

    public void StartPublishing(byte[] compressedPayload, int targetFps, TransportOptions options)
    {
        if (_mqttClient == null || !_mqttClient.IsConnected)
            throw new InvalidOperationException("Not connected to broker.");

        StopPublishing();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(async () =>
        {
            double targetFrameTimeMs = 1000.0 / targetFps;
            var sw = new Stopwatch();
            long frameId = 0;

            while (!token.IsCancellationRequested)
            {
                sw.Restart();
                
                long currentTimestamp = Stopwatch.GetTimestamp();
                BitConverter.TryWriteBytes(compressedPayload.AsSpan(0, 8), currentTimestamp);
                BitConverter.TryWriteBytes(compressedPayload.AsSpan(8, 8), frameId);

                if (options.ChunkSizeMb > 0)
                {
                    int chunkBytes = options.ChunkSizeMb * 1024 * 1024;
                    int chunksCount = (int)Math.Ceiling((double)compressedPayload.Length / chunkBytes);

                    if (options.ParallelChunkPublish)
                    {
                        var tasks = new Task[chunksCount];
                        for (int i = 0; i < chunksCount; i++)
                        {
                            int offset = i * chunkBytes;
                            int length = Math.Min(chunkBytes, compressedPayload.Length - offset);

                            var message = new MqttApplicationMessageBuilder()
                                .WithTopic($"testbench/image/chunk/{i}")
                                .WithPayload(new ArraySegment<byte>(compressedPayload, offset, length))
                                .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)options.Qos)
                                .Build();

                            tasks[i] = _mqttClient.PublishAsync(message, token);
                        }
                        await Task.WhenAll(tasks);
                    }
                    else
                    {
                        for (int i = 0; i < chunksCount; i++)
                        {
                            int offset = i * chunkBytes;
                            int length = Math.Min(chunkBytes, compressedPayload.Length - offset);

                            var message = new MqttApplicationMessageBuilder()
                                .WithTopic($"testbench/image/chunk/{i}")
                                .WithPayload(new ArraySegment<byte>(compressedPayload, offset, length))
                                .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)options.Qos)
                                .Build();

                            await _mqttClient.PublishAsync(message, token);
                        }
                    }
                }
                else
                {
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic("testbench/image/full")
                        .WithPayload(new ArraySegment<byte>(compressedPayload, 0, compressedPayload.Length))
                        .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)options.Qos)
                        .Build();

                    await _mqttClient.PublishAsync(message, token);
                }

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

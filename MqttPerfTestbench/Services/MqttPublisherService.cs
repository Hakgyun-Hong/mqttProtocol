using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MqttPerfTestbench.Models;

namespace MqttPerfTestbench.Services;

public class MqttPublisherService
{
    private IMqttClient? _mqttClient;
    private CancellationTokenSource? _cts;
    private readonly PerfMetrics _metrics;
    
    public bool IsRunning => _cts != null && !_cts.IsCancellationRequested;

    public MqttPublisherService(PerfMetrics metrics)
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

    public void StartPublishing(int payloadSizeMb, int targetFps, int chunkSizeMb = 0, int qos = 0, bool parallelChunkPublish = false)
    {
        if (_mqttClient == null || !_mqttClient.IsConnected)
            throw new InvalidOperationException("Not connected to broker.");

        StopPublishing();
        _cts = new CancellationTokenSource();
        _metrics.Reset();

        _ = Task.Run(() => PublishLoopAsync(payloadSizeMb, targetFps, chunkSizeMb, qos, parallelChunkPublish, _cts.Token), _cts.Token);
    }

    public void StopPublishing()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;
    }

    private async Task PublishLoopAsync(int payloadSizeMb, int targetFps, int chunkSizeMb, int qos, bool parallelChunkPublish, CancellationToken ct)
    {
        int payloadBytes = payloadSizeMb * 1024 * 1024;
        byte[] buffer = MemoryBufferPool.Rent(payloadBytes);
        
        // Fill dummy data
        Array.Fill(buffer, (byte)1);

        double targetFrameTimeMs = 1000.0 / targetFps;
        var sw = new Stopwatch();

        try
        {
            long frameId = 0;
            while (!ct.IsCancellationRequested)
            {
                sw.Restart();
                
                // Embed Timestamp (8 bytes) and FrameID (8 bytes) at the start of payload to measure Latency
                long currentTimestamp = Stopwatch.GetTimestamp();
                BitConverter.TryWriteBytes(buffer.AsSpan(0, 8), currentTimestamp);
                BitConverter.TryWriteBytes(buffer.AsSpan(8, 8), frameId);

                if (chunkSizeMb > 0 && chunkSizeMb < payloadSizeMb)
                {
                    // Chunked Transmission
                    int chunkBytes = chunkSizeMb * 1024 * 1024;
                    int chunksCount = (int)Math.Ceiling((double)payloadBytes / chunkBytes);

                    if (parallelChunkPublish)
                    {
                        var tasks = new Task[chunksCount];
                        for (int i = 0; i < chunksCount; i++)
                        {
                            int offset = i * chunkBytes;
                            int length = Math.Min(chunkBytes, payloadBytes - offset);

                            var message = new MqttApplicationMessageBuilder()
                                .WithTopic($"testbench/image/chunk/{i}")
                                .WithPayload(new ArraySegment<byte>(buffer, offset, length))
                                .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)qos)
                                .Build();

                            tasks[i] = _mqttClient.PublishAsync(message, ct);
                        }
                        await Task.WhenAll(tasks);
                    }
                    else
                    {
                        for (int i = 0; i < chunksCount; i++)
                        {
                            int offset = i * chunkBytes;
                            int length = Math.Min(chunkBytes, payloadBytes - offset);

                            // Note: Using ReadOnlyMemory avoids copying the buffer for the payload
                            var message = new MqttApplicationMessageBuilder()
                                .WithTopic($"testbench/image/chunk/{i}")
                                .WithPayload(new ArraySegment<byte>(buffer, offset, length))
                                .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)qos)
                                .Build();

                            await _mqttClient.PublishAsync(message, ct);
                        }
                    }
                }
                else
                {
                    // Single Packet Transmission
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic("testbench/image/full")
                        .WithPayload(new ArraySegment<byte>(buffer, 0, payloadBytes))
                        .WithQualityOfServiceLevel((MQTTnet.Protocol.MqttQualityOfServiceLevel)qos)
                        .Build();

                    await _mqttClient.PublishAsync(message, ct);
                }

                _metrics.AddFrame(payloadBytes);
                frameId++;

                sw.Stop();
                double elapsedMs = sw.Elapsed.TotalMilliseconds;
                if (elapsedMs < targetFrameTimeMs)
                {
                    // Throttle to hit target FPS
                    int delayMs = (int)(targetFrameTimeMs - elapsedMs);
                    if (delayMs > 0)
                    {
                        await Task.Delay(delayMs, ct);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            MemoryBufferPool.Return(buffer);
        }
    }
}

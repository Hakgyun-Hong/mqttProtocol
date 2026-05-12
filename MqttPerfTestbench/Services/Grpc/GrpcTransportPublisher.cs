using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using ProtoBuf.Grpc.Client;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.Grpc;

public class GrpcTransportPublisher : ITransportPublisher
{
    private GrpcChannel? _channel;
    private IGrpcImageService? _client;
    private CancellationTokenSource? _cts;

    public Task ConnectAsync(TransportOptions options)
    {
        var channelOptions = new GrpcChannelOptions
        {
            MaxSendMessageSize = options.MaxMessageSizeMb * 1024 * 1024,
            MaxReceiveMessageSize = options.MaxMessageSizeMb * 1024 * 1024
        };

        _channel = GrpcChannel.ForAddress($"http://{options.Server}:{options.Port}", channelOptions);
        _client = _channel.CreateGrpcService<IGrpcImageService>();
        
        return Task.CompletedTask;
    }

    public async Task DisconnectAsync()
    {
        StopPublishing();
        if (_channel != null)
        {
            await _channel.ShutdownAsync();
            _channel.Dispose();
            _channel = null;
        }
    }

    public void StartPublishing(byte[] compressedPayload, int targetFps, TransportOptions options)
    {
        if (_client == null) throw new InvalidOperationException("gRPC Client not connected.");

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
                            byte[] chunk = new byte[length];
                            Array.Copy(compressedPayload, offset, chunk, 0, length);
                            tasks[i] = _client.SendChunkAsync(chunk);
                        }
                        await Task.WhenAll(tasks);
                    }
                    else
                    {
                        for (int i = 0; i < chunksCount; i++)
                        {
                            int offset = i * chunkBytes;
                            int length = Math.Min(chunkBytes, compressedPayload.Length - offset);
                            byte[] chunk = new byte[length];
                            Array.Copy(compressedPayload, offset, chunk, 0, length);
                            await _client.SendChunkAsync(chunk);
                        }
                    }
                }
                else
                {
                    await _client.SendChunkAsync(compressedPayload);
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

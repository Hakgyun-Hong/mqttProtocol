using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MqttPerfTestbench.Core.Services.Grpc;

public class GrpcImageService : IGrpcImageService
{
    public static event Action<string>? OnClientConnected;
    public static event Action<string>? OnClientDisconnected;
    
    private static readonly List<ChannelWriter<ImageFrame>> _subscribers = new();
    private static readonly object _lock = new();

    public static void BroadcastFrame(byte[] payload)
    {
        lock (_lock)
        {
            var frame = new ImageFrame { Payload = payload };
            foreach (var sub in _subscribers)
            {
                sub.TryWrite(frame);
            }
        }
    }

    public async IAsyncEnumerable<ImageFrame> SubscribeFramesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string clientId = $"gRPC-Client-{Guid.NewGuid().ToString()[..8]}";
        OnClientConnected?.Invoke(clientId);

        var channel = Channel.CreateBounded<ImageFrame>(new BoundedChannelOptions(5)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });

        lock (_lock)
        {
            _subscribers.Add(channel.Writer);
        }

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ImageFrame frame;
                try
                {
                    frame = await channel.Reader.ReadAsync(cancellationToken);
                }
                catch (ChannelClosedException)
                {
                    break;
                }
                yield return frame;
            }
        }
        finally
        {
            lock (_lock)
            {
                _subscribers.Remove(channel.Writer);
            }
            OnClientDisconnected?.Invoke(clientId);
        }
    }
}

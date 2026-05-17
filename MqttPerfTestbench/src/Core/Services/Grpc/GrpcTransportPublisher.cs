using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ProtoBuf.Grpc.Server;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Grpc;

public sealed class GrpcTransportPublisher : ITransportPublisher
{
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    private WebApplication? _app;
    private CancellationTokenSource? _cts;

    public async Task OpenAsync(TransportOptions options)
    {
        var builder = WebApplication.CreateBuilder();
        
        // Disable verbose logging for high performance
        builder.Logging.ClearProviders();

        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Any, options.Port, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http2;
            });
            // Configure maximum message size
            kestrel.Limits.MaxRequestBodySize = 100 * 1024 * 1024;
        });

        builder.Services.AddCodeFirstGrpc(grpcOptions =>
        {
            grpcOptions.MaxReceiveMessageSize = 100 * 1024 * 1024;
            grpcOptions.MaxSendMessageSize = 100 * 1024 * 1024;
        });

        _app = builder.Build();
        _app.MapGrpcService<GrpcImageService>();

        // Wire up connection events
        GrpcImageService.OnClientConnected += OnGrpcClientConnected;
        GrpcImageService.OnClientDisconnected += OnGrpcClientDisconnected;

        await _app.StartAsync();
        Console.WriteLine($"[gRPC Publisher] Server listening on port {options.Port}");
    }

    private void OnGrpcClientConnected(string ep) => ClientConnected?.Invoke(ep);
    private void OnGrpcClientDisconnected(string ep) => ClientDisconnected?.Invoke(ep);

    public async Task CloseAsync()
    {
        StopPublishing();

        GrpcImageService.OnClientConnected -= OnGrpcClientConnected;
        GrpcImageService.OnClientDisconnected -= OnGrpcClientDisconnected;

        if (_app != null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
            _app = null;
        }
    }

    public void StartPublishing(byte[] payload, int targetFps, TransportOptions options)
    {
        StopPublishing();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(async () =>
        {
            double msPerFrame = 1000.0 / targetFps;
            var sw = new Stopwatch();
            long frameId = 0;

            while (!token.IsCancellationRequested)
            {
                sw.Restart();
                
                // Write timestamps and metadata
                BitConverter.TryWriteBytes(payload.AsSpan(0, 8), Stopwatch.GetTimestamp());
                BitConverter.TryWriteBytes(payload.AsSpan(8, 8), frameId++);

                GrpcImageService.BroadcastFrame(payload);

                int delay = (int)(msPerFrame - sw.ElapsedMilliseconds);
                if (delay > 0)
                {
                    await Task.Delay(delay, token);
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

    public void Dispose() => CloseAsync().GetAwaiter().GetResult();
}

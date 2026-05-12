using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using ProtoBuf.Grpc.Server;
using MqttPerfTestbench.Models;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.Grpc;

public class GrpcTransportSubscriber : ITransportSubscriber
{
    private WebApplication? _app;
    private readonly PerfMetrics _metrics;

    public GrpcTransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public async Task ConnectAsync(TransportOptions options)
    {
        var builder = WebApplication.CreateBuilder();
        
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Parse(options.Server), options.Port, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http2;
            });
            kestrel.Limits.MaxRequestBodySize = options.MaxMessageSizeMb * 1024 * 1024;
        });

        builder.Services.AddSingleton(_metrics);
        builder.Services.AddCodeFirstGrpc(grpcOptions =>
        {
            grpcOptions.MaxReceiveMessageSize = options.MaxMessageSizeMb * 1024 * 1024;
            grpcOptions.MaxSendMessageSize = options.MaxMessageSizeMb * 1024 * 1024;
        });

        _app = builder.Build();
        _app.MapGrpcService<GrpcImageService>();
        
        await _app.StartAsync();
    }

    public async Task DisconnectAsync()
    {
        if (_app != null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
            _app = null;
        }
    }
}

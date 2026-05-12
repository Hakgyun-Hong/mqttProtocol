using System;
using System.Diagnostics;
using System.Threading.Tasks;
using MqttPerfTestbench.Models;

namespace MqttPerfTestbench.Services.Grpc;

public class GrpcImageService : IGrpcImageService
{
    private readonly PerfMetrics _metrics;

    public GrpcImageService(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public Task SendChunkAsync(byte[] payload)
    {
        if (payload != null && payload.Length >= 16)
        {
            long startTimestamp = BitConverter.ToInt64(payload, 0);
            long currentTimestamp = Stopwatch.GetTimestamp();
            double latencyMs = (currentTimestamp - startTimestamp) * 1000.0 / Stopwatch.Frequency;
            
            _metrics.AddFrame(payload.Length, (long)latencyMs);
        }
        return Task.CompletedTask;
    }
}

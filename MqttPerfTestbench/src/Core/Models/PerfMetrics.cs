using System;
using System.Diagnostics;
using System.Threading;

namespace MqttPerfTestbench.Core.Models;

public class PerfMetrics
{
    private long _framesTransferred;
    private long _bytesTransferred;
    private long _totalLatencyMs;

    public long FramesTransferred => Interlocked.Read(ref _framesTransferred);
    public long BytesTransferred => Interlocked.Read(ref _bytesTransferred);
    
    // Average Latency
    public double AverageLatencyMs => FramesTransferred == 0 ? 0 : (double)Interlocked.Read(ref _totalLatencyMs) / FramesTransferred;

    public void AddFrame(long bytesCount, long latencyMs = 0)
    {
        Interlocked.Increment(ref _framesTransferred);
        Interlocked.Add(ref _bytesTransferred, bytesCount);
        if (latencyMs > 0)
        {
            Interlocked.Add(ref _totalLatencyMs, latencyMs);
        }
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _framesTransferred, 0);
        Interlocked.Exchange(ref _bytesTransferred, 0);
        Interlocked.Exchange(ref _totalLatencyMs, 0);
    }
}

using System;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttPerfTestbench.Models;

/// <summary>
/// Zero-allocation buffer pool wrapper.
/// Used to fetch and return buffers without triggering LOH fragmentation.
/// </summary>
public static class MemoryBufferPool
{
    // Max Array Size: 100MB to cover up to 64MB comfortably without massive resizing.
    public static byte[] Rent(int size)
    {
        return ArrayPool<byte>.Shared.Rent(size);
    }

    public static void Return(byte[] buffer)
    {
        if (buffer != null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}

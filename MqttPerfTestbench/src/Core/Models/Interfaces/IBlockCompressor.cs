using System;

namespace MqttPerfTestbench.Core.Models.Interfaces;

public interface IBlockCompressor
{
    string Name { get; }
    
    /// <summary>
    /// Compress the input buffer and return the compressed data.
    /// </summary>
    byte[] Compress(ReadOnlySpan<byte> input, int level);
    
    /// <summary>
    /// Decompress the input buffer into the output buffer.
    /// </summary>
    void Decompress(ReadOnlySpan<byte> input, Span<byte> output);
}

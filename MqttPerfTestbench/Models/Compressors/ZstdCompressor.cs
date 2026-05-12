using System;
using MqttPerfTestbench.Models.Interfaces;
using ZstdSharp;

namespace MqttPerfTestbench.Models.Compressors;

public class ZstdCompressor : IBlockCompressor
{
    public string Name => "Zstd";

    public byte[] Compress(ReadOnlySpan<byte> input, int level)
    {
        using var compressor = new Compressor(level);
        return compressor.Wrap(input).ToArray();
    }

    public void Decompress(ReadOnlySpan<byte> input, Span<byte> output)
    {
        using var decompressor = new Decompressor();
        decompressor.Unwrap(input, output);
    }
}

using System;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Models.Compressors;

public class NoneCompressor : IBlockCompressor
{
    public string Name => "None";

    public byte[] Compress(ReadOnlySpan<byte> input, int level)
    {
        return input.ToArray();
    }

    public void Decompress(ReadOnlySpan<byte> input, Span<byte> output)
    {
        input.CopyTo(output);
    }
}

using System;
using MqttPerfTestbench.Models.Interfaces;
using K4os.Compression.LZ4;

namespace MqttPerfTestbench.Models.Compressors;

public class Lz4Compressor : IBlockCompressor
{
    public string Name => "LZ4";

    public byte[] Compress(ReadOnlySpan<byte> input, int level)
    {
        // LZ4 Level mapping (0: Fast, 9: High)
        LZ4Level lz4Level = level > 5 ? LZ4Level.L10_OPT : LZ4Level.L00_FAST;
        
        int maxLength = LZ4Codec.MaximumOutputSize(input.Length);
        byte[] output = new byte[maxLength];
        
        int encodedLength = LZ4Codec.Encode(input, output, lz4Level);
        
        // Resize array to actual encoded length
        Array.Resize(ref output, encodedLength);
        return output;
    }

    public void Decompress(ReadOnlySpan<byte> input, Span<byte> output)
    {
        LZ4Codec.Decode(input, output);
    }
}

using System;
using System.IO;
using System.IO.Compression;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Models.Compressors;

public class LzwCompressor : IBlockCompressor
{
    public string Name => "LZW (Deflate approx)";

    public byte[] Compress(ReadOnlySpan<byte> input, int level)
    {
        using var ms = new MemoryStream();
        using (var deflate = new DeflateStream(ms, CompressionLevel.Fastest))
        {
            deflate.Write(input);
        }
        return ms.ToArray();
    }

    public void Decompress(ReadOnlySpan<byte> input, Span<byte> output)
    {
        using var ms = new MemoryStream(input.ToArray());
        using var deflate = new DeflateStream(ms, CompressionMode.Decompress);
        
        int read;
        int offset = 0;
        while ((read = deflate.Read(output.Slice(offset))) > 0)
        {
            offset += read;
        }
    }
}

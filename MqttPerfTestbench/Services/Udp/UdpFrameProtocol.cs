using System;
using System.Buffers.Binary;

namespace MqttPerfTestbench.Services.Udp;

internal static class UdpFrameProtocol
{
    public const uint Magic = 0x3150464D; // "MFP1" little-endian-ish marker
    public const byte Version = 1;

    public const int HeaderSize = 40;

    // Increased to 8KB (Jumbo Frame-ish) to reduce syscall overhead for massive data.
    // Standard Ethernet MTU is 1500, but for local performance testing, 
    // 8192 significantly improves throughput by reducing Send() calls.
    public const int MaxUdpPayloadSize = 8192;

    public const int MaxChunkPayloadSize = MaxUdpPayloadSize - HeaderSize;

    /*
        Header layout, little endian:

        0   uint32  magic
        4   byte    version
        5   byte    flags
        6   uint16  headerSize
        8   int64   frameId
        16  int64   timestampStopwatchTicks
        24  int32   payloadLength
        28  int32   chunkIndex
        32  int32   chunkCount
        36  uint16  chunkPayloadSize
        38  uint16  reserved
    */

    public static void WriteHeader(
        Span<byte> destination,
        long frameId,
        long timestamp,
        int payloadLength,
        int chunkIndex,
        int chunkCount,
        int chunkPayloadSize)
    {
        if (destination.Length < HeaderSize)
            throw new ArgumentException("Header buffer is too small.", nameof(destination));

        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), Magic);
        destination[4] = Version;
        destination[5] = 0;

        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(6, 2), HeaderSize);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(8, 8), frameId);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(16, 8), timestamp);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(24, 4), payloadLength);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(28, 4), chunkIndex);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(32, 4), chunkCount);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(36, 2), checked((ushort)chunkPayloadSize));
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(38, 2), 0);
    }

    public static bool TryReadHeader(
        ReadOnlySpan<byte> source,
        out UdpChunkHeader header)
    {
        header = default;

        if (source.Length < HeaderSize)
            return false;

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(0, 4));
        if (magic != Magic)
            return false;

        byte version = source[4];
        if (version != Version)
            return false;

        ushort headerSize = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(6, 2));
        if (headerSize != HeaderSize)
            return false;

        long frameId = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(8, 8));
        long timestamp = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(16, 8));
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(24, 4));
        int chunkIndex = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(28, 4));
        int chunkCount = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(32, 4));
        ushort chunkPayloadSize = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(36, 2));

        if (payloadLength <= 0)
            return false;

        if (chunkPayloadSize == 0 || chunkPayloadSize > 65535 - HeaderSize)
            return false;

        if (chunkCount <= 0)
            return false;

        if (chunkIndex < 0 || chunkIndex >= chunkCount)
            return false;

        header = new UdpChunkHeader(
            FrameId: frameId,
            Timestamp: timestamp,
            PayloadLength: payloadLength,
            ChunkIndex: chunkIndex,
            ChunkCount: chunkCount,
            ChunkPayloadSize: chunkPayloadSize);

        return true;
    }
}

internal readonly record struct UdpChunkHeader(
    long FrameId,
    long Timestamp,
    int PayloadLength,
    int ChunkIndex,
    int ChunkCount,
    int ChunkPayloadSize);

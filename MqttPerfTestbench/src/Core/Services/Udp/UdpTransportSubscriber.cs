using System;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Udp;

public sealed class UdpTransportSubscriber : ITransportSubscriber
{
    private readonly PerfMetrics _metrics;

    private Socket? _socket;
    private CancellationTokenSource? _cts;
    private Task? _receivingTask;

    public UdpTransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    }

    public Task ConnectAsync(TransportOptions options)
    {
        if (options.Port <= 0 || options.Port > 65535)
            throw new ArgumentOutOfRangeException(nameof(options.Port));

        IPAddress bindAddress;

        if (string.IsNullOrWhiteSpace(options.Server) ||
            options.Server == "*" ||
            options.Server == "0.0.0.0")
        {
            bindAddress = IPAddress.Any;
        }
        else
        {
            bindAddress = IPAddress.Parse(options.Server);
        }

        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

        try 
        {
            _socket.ReceiveBufferSize = Math.Max(
                16 * 1024 * 1024,
                options.HighWatermark > 0 ? options.HighWatermark * 1024 : 16 * 1024 * 1024);
        }
        catch { /* OS limit reached */ }

        _socket.Bind(new IPEndPoint(bindAddress, options.Port));

        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        _receivingTask = Task.Run(() =>
        {
            ReceiveLoop(_socket, _metrics, token);
        }, token);

        return Task.CompletedTask;
    }

    public async Task DisconnectAsync()
    {
        var cts = _cts;

        if (cts != null)
        {
            try   { cts.Cancel(); }
            finally { _cts = null; }
        }

        _socket?.Dispose();

        if (_receivingTask != null)
        {
            try   { await _receivingTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            finally { _receivingTask = null; }
        }

        cts?.Dispose();
        _socket = null;
    }

    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();

    private static void ReceiveLoop(
        Socket socket,
        PerfMetrics metrics,
        CancellationToken token)
    {
        byte[] packet = ArrayPool<byte>.Shared.Rent(UdpFrameProtocol.MaxUdpPayloadSize);
        EndPoint remote = new IPEndPoint(IPAddress.Any, 0);

        FrameAssembler? currentFrame = null;

        try
        {
            while (!token.IsCancellationRequested)
            {
                int received;

                try
                {
                    received = socket.ReceiveFrom(packet, 0, packet.Length, SocketFlags.None, ref remote);
                }
                catch (SocketException)
                {
                    if (token.IsCancellationRequested)
                        break;

                    continue;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                if (received < UdpFrameProtocol.HeaderSize)
                    continue;

                ReadOnlySpan<byte> receivedSpan = packet.AsSpan(0, received);

                if (!UdpFrameProtocol.TryReadHeader(receivedSpan, out var header))
                    continue;

                int chunkDataLength = received - UdpFrameProtocol.HeaderSize;
                if (chunkDataLength <= 0)
                    continue;

                if (header.PayloadLength <= 0)
                    continue;

                if (header.ChunkCount <= 0)
                    continue;

                // Old frame: ignore.
                if (currentFrame != null && header.FrameId < currentFrame.FrameId)
                    continue;

                // Newer frame arrived before previous frame completed.
                // For latest-frame realtime mode, drop previous incomplete frame.
                if (currentFrame == null || header.FrameId > currentFrame.FrameId)
                {
                    currentFrame?.Dispose();

                    currentFrame = new FrameAssembler(
                        header.FrameId,
                        header.Timestamp,
                        header.PayloadLength,
                        header.ChunkCount,
                        header.ChunkPayloadSize);
                }

                if (currentFrame.FrameId != header.FrameId)
                    continue;

                bool accepted = currentFrame.TryAcceptChunk(header, receivedSpan.Slice(UdpFrameProtocol.HeaderSize));

                if (!accepted)
                    continue;

                if (currentFrame.IsComplete)
                {
                    long now = Stopwatch.GetTimestamp();
                    double latencyMs = (now - currentFrame.Timestamp) * 1000.0 / Stopwatch.Frequency;

                    metrics.AddFrame(currentFrame.PayloadLength, (long)latencyMs);

                    currentFrame.Dispose();
                    currentFrame = null;
                }
            }
        }
        finally
        {
            currentFrame?.Dispose();
            ArrayPool<byte>.Shared.Return(packet);
        }
    }

    private sealed class FrameAssembler : IDisposable
    {
        private readonly byte[] _buffer;
        private readonly bool[] _receivedChunks;
        private int _receivedChunkCount;
        private bool _disposed;

        public long FrameId { get; }
        public long Timestamp { get; }
        public int PayloadLength { get; }
        public int ChunkCount { get; }
        public int ChunkPayloadSize { get; }

        public bool IsComplete => _receivedChunkCount == ChunkCount;

        public FrameAssembler(
            long frameId,
            long timestamp,
            int payloadLength,
            int chunkCount,
            int chunkPayloadSize)
        {
            if (payloadLength <= 0)
                throw new ArgumentOutOfRangeException(nameof(payloadLength));

            if (chunkCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(chunkCount));

            if (chunkPayloadSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(chunkPayloadSize));

            FrameId = frameId;
            Timestamp = timestamp;
            PayloadLength = payloadLength;
            ChunkCount = chunkCount;
            ChunkPayloadSize = chunkPayloadSize;

            _buffer = ArrayPool<byte>.Shared.Rent(payloadLength);
            _receivedChunks = new bool[chunkCount];
        }

        public bool TryAcceptChunk(
            UdpChunkHeader header,
            ReadOnlySpan<byte> chunkData)
        {
            if (_disposed)
                return false;

            if (header.FrameId != FrameId)
                return false;

            if (header.Timestamp != Timestamp)
                return false;

            if (header.PayloadLength != PayloadLength)
                return false;

            if (header.ChunkCount != ChunkCount)
                return false;

            if (header.ChunkPayloadSize != ChunkPayloadSize)
                return false;

            int chunkIndex = header.ChunkIndex;
            if ((uint)chunkIndex >= (uint)ChunkCount)
                return false;

            if (_receivedChunks[chunkIndex])
                return false;

            int payloadOffset = chunkIndex * ChunkPayloadSize;
            if (payloadOffset >= PayloadLength)
                return false;

            int expectedMaxLength = Math.Min(ChunkPayloadSize, PayloadLength - payloadOffset);
            int copyLength = Math.Min(expectedMaxLength, chunkData.Length);

            if (copyLength <= 0)
                return false;

            chunkData.Slice(0, copyLength).CopyTo(_buffer.AsSpan(payloadOffset, copyLength));

            _receivedChunks[chunkIndex] = true;
            _receivedChunkCount++;

            return true;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }
}

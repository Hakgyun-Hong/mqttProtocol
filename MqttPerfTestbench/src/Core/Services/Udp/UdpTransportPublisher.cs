using System;
using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Udp;

public sealed class UdpTransportPublisher : ITransportPublisher
{
    // UDP는 연결 개념 없음 → 이벤트 미구현
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    private Socket? _socket;
    private EndPoint? _remoteEndPoint;
    private CancellationTokenSource? _cts;
    private Task? _publishingTask;

    public Task OpenAsync(TransportOptions options)
    {
        if (options.Port <= 0 || options.Port > 65535)
            throw new ArgumentOutOfRangeException(nameof(options.Port));

        // UDP Publisher는 Server 주소로 직접 전송 (서버 역할 없음)
        string target = options.Server == "0.0.0.0" ? "127.0.0.1" : options.Server;
        var addresses = Dns.GetHostAddresses(target);
        if (addresses.Length == 0)
            throw new InvalidOperationException($"Unable to resolve UDP target: {target}");

        var address = Array.Find(addresses, x => x.AddressFamily == AddressFamily.InterNetwork)
                      ?? addresses[0];

        _remoteEndPoint = new IPEndPoint(address, options.Port);
        _socket = new Socket(address.AddressFamily, SocketType.Dgram, ProtocolType.Udp);

        try 
        {
            _socket.SendBufferSize = Math.Max(
                4 * 1024 * 1024,
                options.HighWatermark > 0 ? options.HighWatermark * 1024 : 4 * 1024 * 1024);
        }
        catch { /* OS limit reached */ }

        _socket.Connect(_remoteEndPoint);
        Console.WriteLine($"[UDP Publisher] Ready to send to {_remoteEndPoint}");
        return Task.CompletedTask;
    }

    public async Task CloseAsync()
    {
        StopPublishing();

        if (_publishingTask != null)
        {
            try   { await _publishingTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            finally { _publishingTask = null; }
        }

        _socket?.Dispose();
        _socket = null;
        _remoteEndPoint = null;
    }

    public void Dispose() => CloseAsync().GetAwaiter().GetResult();

    public void StartPublishing(byte[] compressedPayload, int targetFps, TransportOptions options)
    {
        if (_socket == null)
            throw new InvalidOperationException("UDP socket is not connected.");

        if (compressedPayload == null)
            throw new ArgumentNullException(nameof(compressedPayload));

        if (compressedPayload.Length <= 0)
            throw new ArgumentException("Payload is empty.", nameof(compressedPayload));

        if (targetFps <= 0)
            throw new ArgumentOutOfRangeException(nameof(targetFps));

        StopPublishing();

        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        _publishingTask = Task.Run(() =>
        {
            PublishLoop(_socket, compressedPayload, targetFps, token);
        }, token);
    }

    public void StopPublishing()
    {
        var cts = _cts;
        if (cts == null)
            return;

        try
        {
            cts.Cancel();
        }
        finally
        {
            _cts = null;
            cts.Dispose();
        }
    }

    private static void PublishLoop(
        Socket socket,
        byte[] payload,
        int targetFps,
        CancellationToken token)
    {
        double targetFrameTimeMs = 1000.0 / targetFps;

        int chunkPayloadSize = UdpFrameProtocol.MaxChunkPayloadSize;
        int chunkCount = (payload.Length + chunkPayloadSize - 1) / chunkPayloadSize;

        byte[] packet = ArrayPool<byte>.Shared.Rent(UdpFrameProtocol.MaxUdpPayloadSize);

        var frameStopwatch = new Stopwatch();
        long frameId = 0;

        try
        {
            while (!token.IsCancellationRequested)
            {
                frameStopwatch.Restart();

                long timestamp = Stopwatch.GetTimestamp();

                // Prepare chunks for the entire frame
                int totalPackets = chunkCount;
                int packetsSent = 0;

                for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
                {
                    if (token.IsCancellationRequested) break;

                    int payloadOffset = chunkIndex * chunkPayloadSize;
                    int bytesRemaining = payload.Length - payloadOffset;
                    int bytesThisChunk = Math.Min(chunkPayloadSize, bytesRemaining);

                    // We use a local buffer per chunk to allow some degree of parallel preparation if needed,
                    // but for now, reuse the packet buffer to save allocations.
                    UdpFrameProtocol.WriteHeader(
                        packet.AsSpan(0, UdpFrameProtocol.HeaderSize),
                        frameId,
                        timestamp,
                        payload.Length,
                        chunkIndex,
                        chunkCount,
                        chunkPayloadSize);

                    Buffer.BlockCopy(
                        payload,
                        payloadOffset,
                        packet,
                        UdpFrameProtocol.HeaderSize,
                        bytesThisChunk);

                    int packetLength = UdpFrameProtocol.HeaderSize + bytesThisChunk;

                    try
                    {
                        // Use Send to minimize async overhead for small packets,
                        // but don't check return value every time to keep the pipe full.
                        socket.Send(packet, 0, packetLength, SocketFlags.None);
                        packetsSent++;
                    }
                    catch (SocketException ex) when (ex.SocketErrorCode == SocketError.NoBufferSpaceAvailable)
                    {
                        // Kernel buffer full, wait a tiny bit
                        Thread.SpinWait(100);
                        chunkIndex--; // Retry this chunk
                        continue;
                    }
                    
                    // Periodically yield to prevent UI freeze and allow other tasks
                    if (packetsSent % 500 == 0)
                    {
                        Thread.Yield();
                    }
                }

                frameId++;

                frameStopwatch.Stop();

                double elapsedMs = frameStopwatch.Elapsed.TotalMilliseconds;
                double delayMs = targetFrameTimeMs - elapsedMs;

                if (delayMs > 1)
                {
                    Task.Delay((int)delayMs, token).GetAwaiter().GetResult();
                }
                else
                {
                    // If sending already took longer than frame budget,
                    // immediately continue with next latest frame.
                    Thread.Yield();
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(packet);
        }
    }
}

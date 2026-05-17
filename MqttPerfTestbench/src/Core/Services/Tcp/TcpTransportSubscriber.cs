using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Tcp;

public sealed class TcpTransportSubscriber : ITransportSubscriber
{
    private readonly PerfMetrics _metrics;
    private Socket? _socket;
    private CancellationTokenSource? _cts;

    public TcpTransportSubscriber(PerfMetrics metrics) => _metrics = metrics;

    public async Task ConnectAsync(TransportOptions options)
    {
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _socket.NoDelay = options.TcpNoDelay;
        try { _socket.ReceiveBufferSize = options.BufferSizeMb * 1024 * 1024; } catch { }

        for (int i = 0; i < 10; i++)
        {
            try
            {
                await _socket.ConnectAsync(new IPEndPoint(IPAddress.Parse(options.Server), options.Port));
                Console.WriteLine($"[TCP Sub] Connected to {options.Server}:{options.Port}");
                break;
            }
            catch (SocketException)
            {
                Console.WriteLine($"[TCP Sub] Connect attempt {i + 1} failed");
                if (i == 9) throw;
                await Task.Delay(1000);
            }
        }

        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        _ = Task.Run(async () =>
        {
            byte[] header = new byte[4];
            while (!token.IsCancellationRequested)
            {
                try
                {
                    if (!await ReadExactAsync(_socket, header, 4, token)) break;
                    int payloadLen = BitConverter.ToInt32(header, 0);
                    byte[] payload = new byte[payloadLen];
                    if (!await ReadExactAsync(_socket, payload, payloadLen, token)) break;

                    long ts = BitConverter.ToInt64(payload, 0);
                    long latencyMs = (long)((Stopwatch.GetTimestamp() - ts) * 1000.0 / Stopwatch.Frequency);
                    _metrics.AddFrame(payloadLen, latencyMs);
                }
                catch (Exception) { break; }
            }
        }, token);
    }

    private static async Task<bool> ReadExactAsync(Socket socket, byte[] buf, int length, CancellationToken ct)
    {
        int total = 0;
        while (total < length)
        {
            int read = await socket.ReceiveAsync(new ArraySegment<byte>(buf, total, length - total), SocketFlags.None, ct);
            if (read == 0) return false;
            total += read;
        }
        return true;
    }

    public Task DisconnectAsync()
    {
        try { _cts?.Cancel(); } catch { }
        try { _cts?.Dispose(); } catch { }
        _cts = null;
        try { _socket?.Dispose(); } catch { }
        _socket = null;
        return Task.CompletedTask;
    }

    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();
}

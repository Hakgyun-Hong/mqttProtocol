using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Models;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.Tcp;

public class TcpTransportSubscriber : ITransportSubscriber
{
    private Socket? _socket;
    private readonly PerfMetrics _metrics;
    private CancellationTokenSource? _cts;

    public TcpTransportSubscriber(PerfMetrics metrics)
    {
        _metrics = metrics;
    }

    public async Task ConnectAsync(TransportOptions options)
    {
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _socket.NoDelay = options.TcpNoDelay;
        _socket.ReceiveBufferSize = options.BufferSizeMb * 1024 * 1024;

        // Try connect with retry since Publisher (Listener) might be starting up
        for (int i = 0; i < 5; i++)
        {
            try
            {
                await _socket.ConnectAsync(new IPEndPoint(IPAddress.Parse(options.Server), options.Port));
                break;
            }
            catch (SocketException)
            {
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
                    int headerRead = await ReadExactAsync(_socket, header, 4, token);
                    if (headerRead == 0) break;

                    int payloadLength = BitConverter.ToInt32(header, 0);
                    byte[] payload = MemoryBufferPool.Rent(payloadLength);
                    
                    int payloadRead = await ReadExactAsync(_socket, payload, payloadLength, token);
                    if (payloadRead == 0) break;

                    long startTimestamp = BitConverter.ToInt64(payload, 0);
                    long currentTimestamp = Stopwatch.GetTimestamp();
                    double latencyMs = (currentTimestamp - startTimestamp) * 1000.0 / Stopwatch.Frequency;
                    
                    _metrics.AddFrame(payloadLength, (long)latencyMs);
                    MemoryBufferPool.Return(payload);
                }
                catch (Exception) { break; }
            }
        }, token);
    }

    private async Task<int> ReadExactAsync(Socket socket, byte[] buffer, int length, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < length)
        {
            int read = await socket.ReceiveAsync(new ArraySegment<byte>(buffer, totalRead, length - totalRead), SocketFlags.None, ct);
            if (read == 0) return 0; // Connection closed
            totalRead += read;
        }
        return totalRead;
    }

    public Task DisconnectAsync()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _socket?.Dispose();
        return Task.CompletedTask;
    }
}

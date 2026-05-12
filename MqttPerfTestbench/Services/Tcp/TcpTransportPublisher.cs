using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Services.Tcp;

public class TcpTransportPublisher : ITransportPublisher
{
    private Socket? _listener;
    private Socket? _clientSocket;
    private CancellationTokenSource? _cts;

    public async Task ConnectAsync(TransportOptions options)
    {
        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.NoDelay = options.TcpNoDelay;
        _listener.SendBufferSize = options.BufferSizeMb * 1024 * 1024;
        
        _listener.Bind(new IPEndPoint(IPAddress.Parse(options.Server), options.Port));
        _listener.Listen(1);
        
        // Wait for subscriber to connect
        _clientSocket = await _listener.AcceptAsync();
        _clientSocket.NoDelay = options.TcpNoDelay;
        _clientSocket.SendBufferSize = options.BufferSizeMb * 1024 * 1024;
    }

    public Task DisconnectAsync()
    {
        StopPublishing();
        _clientSocket?.Dispose();
        _listener?.Dispose();
        return Task.CompletedTask;
    }

    public void StartPublishing(byte[] compressedPayload, int targetFps, TransportOptions options)
    {
        if (_clientSocket == null) throw new InvalidOperationException("TCP Client not connected.");

        StopPublishing();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(async () =>
        {
            double targetFrameTimeMs = 1000.0 / targetFps;
            var sw = new Stopwatch();
            long frameId = 0;
            
            // To frame the TCP stream, send 4 byte length header
            byte[] header = new byte[4];
            BitConverter.TryWriteBytes(header, compressedPayload.Length);

            while (!token.IsCancellationRequested)
            {
                sw.Restart();
                
                long currentTimestamp = Stopwatch.GetTimestamp();
                BitConverter.TryWriteBytes(compressedPayload.AsSpan(0, 8), currentTimestamp);
                BitConverter.TryWriteBytes(compressedPayload.AsSpan(8, 8), frameId);

                try
                {
                    await _clientSocket.SendAsync(header, SocketFlags.None, token);
                    await _clientSocket.SendAsync(compressedPayload, SocketFlags.None, token);
                }
                catch (SocketException) { break; }

                frameId++;
                sw.Stop();
                double elapsedMs = sw.Elapsed.TotalMilliseconds;
                if (elapsedMs < targetFrameTimeMs)
                {
                    int delayMs = (int)(targetFrameTimeMs - elapsedMs);
                    if (delayMs > 0)
                    {
                        await Task.Delay(delayMs, token);
                    }
                }
            }
        }, token);
    }

    public void StopPublishing()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;
    }
}

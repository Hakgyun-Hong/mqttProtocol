using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Services.Tcp;

public sealed class TcpTransportPublisher : ITransportPublisher
{
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    private Socket?  _listener;
    private Socket?  _clientSocket;
    private CancellationTokenSource? _cts;
    private Task?    _acceptTask;

    public async Task OpenAsync(TransportOptions options)
    {
        _cts = new CancellationTokenSource();

        _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _listener.NoDelay = options.TcpNoDelay;
        try { _listener.SendBufferSize = options.BufferSizeMb * 1024 * 1024; } catch { }

        _listener.Bind(new IPEndPoint(IPAddress.Any, options.Port));
        _listener.Listen(5);
        Console.WriteLine($"[TCP Publisher] Listening on port {options.Port}");

        var token = _cts.Token;
        _acceptTask = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var sock = await _listener.AcceptAsync(token);
                    var ep = sock.RemoteEndPoint?.ToString() ?? "unknown";

                    if (_clientSocket != null)
                    {
                        var old = _clientSocket.RemoteEndPoint?.ToString() ?? "unknown";
                        _clientSocket.Dispose();
                        ClientDisconnected?.Invoke(old);
                    }

                    _clientSocket = sock;
                    _clientSocket.NoDelay = options.TcpNoDelay;
                    try { _clientSocket.SendBufferSize = options.BufferSizeMb * 1024 * 1024; } catch { }
                    Console.WriteLine($"[TCP Publisher] Client connected: {ep}");
                    ClientConnected?.Invoke(ep);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    if (!token.IsCancellationRequested)
                        Console.WriteLine($"[TCP Publisher] Accept error: {ex.Message}");
                }
            }
        }, token);

        await Task.CompletedTask;
    }

    public async Task CloseAsync()
    {
        StopPublishing();
        _clientSocket?.Dispose();
        _listener?.Dispose();
        try { await (_acceptTask ?? Task.CompletedTask); } catch { }
    }

    public void StartPublishing(byte[] payload, int targetFps, TransportOptions options)
    {
        StopPublishing();
        _cts ??= new CancellationTokenSource();
        var token = _cts.Token;

        Task.Run(async () =>
        {
            double msPerFrame = 1000.0 / targetFps;
            var sw = new Stopwatch();
            byte[] header = new byte[4];
            BitConverter.TryWriteBytes(header, payload.Length);

            while (!token.IsCancellationRequested)
            {
                var sock = _clientSocket;
                if (sock == null) { await Task.Delay(100, token); continue; }

                sw.Restart();
                BitConverter.TryWriteBytes(payload.AsSpan(0, 8), Stopwatch.GetTimestamp());

                try
                {
                    await sock.SendAsync(header, SocketFlags.None, token);
                    await sock.SendAsync(payload, SocketFlags.None, token);
                }
                catch (SocketException) { /* 클라이언트 끊김 */ }

                int delay = (int)(msPerFrame - sw.ElapsedMilliseconds);
                if (delay > 0) await Task.Delay(delay, token);
            }
        }, token);
    }

    public void StopPublishing()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;
    }

    public void Dispose() => CloseAsync().GetAwaiter().GetResult();
}

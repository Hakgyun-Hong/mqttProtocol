using System;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;
using MqttPerfTestbench.Core.Services.Mqtt;
using MqttPerfTestbench.Core.Services.Tcp;
using MqttPerfTestbench.Core.Services.Zmq;
using MqttPerfTestbench.Core.Services.H264;
using MqttPerfTestbench.Core.Services.H265;
using MqttPerfTestbench.Core.Services.Udp;
using MqttPerfTestbench.Core.Services.Grpc;

namespace MqttPerfTestbench.Client.ViewModels;

public partial class SubscriberViewModel : ObservableObject
{
    private readonly PerfMetrics _metrics = new();
    private ITransportSubscriber? _subscriber;
    private readonly DispatcherTimer _timer;

    private long _lastFrames;
    private long _lastBytes;
    private DateTime _lastTime;

    public ObservableCollection<string> Protocols { get; } =
        new(new[] { "MQTT", "ZMQ", "TCP", "UDP", "H.264", "H.265", "gRPC" });

    [ObservableProperty] private string _selectedProtocol = "TCP";
    [ObservableProperty] private string _serverIp   = "127.0.0.1";
    [ObservableProperty] private int    _port        = ProtocolPorts.Get("TCP");
    [ObservableProperty] private int    _imageWidth  = 8192;
    [ObservableProperty] private int    _imageHeight = 8192;

    [ObservableProperty] private double _currentFps;
    [ObservableProperty] private double _currentBandwidthMb;
    [ObservableProperty] private double _averageLatencyMs;

    [ObservableProperty] private bool   _isRunning;
    [ObservableProperty] private string _connectionStatus = "Disconnected";

    public SubscriberViewModel()
    {
        _timer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
        _timer.Tick += OnTimerTick;
    }

    // 프로토콜 변경 시 포트 자동 갱신
    partial void OnSelectedProtocolChanged(string value)
    {
        Port = ProtocolPorts.Get(value);
    }

    private void OnTimerTick(object? sender, EventArgs e)
    {
        var now     = DateTime.UtcNow;
        var elapsed = (now - _lastTime).TotalSeconds;
        if (elapsed <= 0) return;

        long frames = _metrics.FramesTransferred;
        long bytes  = _metrics.BytesTransferred;

        CurrentFps          = (frames - _lastFrames) / elapsed;
        CurrentBandwidthMb  = ((bytes - _lastBytes) / elapsed) / (1024 * 1024);
        AverageLatencyMs    = _metrics.AverageLatencyMs;

        _lastFrames = frames;
        _lastBytes  = bytes;
        _lastTime   = now;
    }

    [RelayCommand]
    private async Task StartClientAsync()
    {
        if (IsRunning) return;
        IsRunning = true;
        ConnectionStatus = "Connecting...";

        try
        {
            _subscriber?.Dispose();
            _subscriber = SelectedProtocol switch
            {
                "MQTT"  => new MqttTransportSubscriber(_metrics),
                "ZMQ"   => new ZmqTransportSubscriber(_metrics),
                "TCP"   => new TcpTransportSubscriber(_metrics),
                "UDP"   => new UdpTransportSubscriber(_metrics),
                "H.264" => new H264TransportSubscriber(_metrics),
                "H.265" => new H265TransportSubscriber(_metrics),
                "gRPC"  => new GrpcTransportSubscriber(_metrics),
                _       => throw new NotSupportedException()
            };

            var options = new TransportOptions
            {
                Server     = ServerIp,
                Port       = Port,
                Width      = ImageWidth,
                Height     = ImageHeight,
                FfmpegPath = OperatingSystem.IsMacOS() ? "/opt/homebrew/bin/ffmpeg" : "ffmpeg"
            };

            await _subscriber.ConnectAsync(options);

            _metrics.Reset();
            _lastFrames = 0;
            _lastBytes  = 0;
            _lastTime   = DateTime.UtcNow;
            _timer.Start();
            ConnectionStatus = "Connected";
        }
        catch (Exception ex)
        {
            IsRunning = false;
            ConnectionStatus = "Failed";
            Console.WriteLine($"[Client] Connect failed: {ex.Message}");
        }
    }

    [RelayCommand]
    private async Task StopClientAsync()
    {
        if (!IsRunning) return;
        _timer.Stop();
        if (_subscriber != null)
        {
            await _subscriber.DisconnectAsync();
            _subscriber.Dispose();
            _subscriber = null;
        }
        IsRunning = false;
        ConnectionStatus = "Disconnected";
        CurrentFps = 0;
        CurrentBandwidthMb = 0;
        AverageLatencyMs = 0;
    }
}

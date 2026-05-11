using System;
using System.Threading.Tasks;
using System.Windows.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using MqttPerfTestbench.Models;
using MqttPerfTestbench.Services;

namespace MqttPerfTestbench.ViewModels;

public partial class MainViewModel : ObservableObject
{
    private readonly PerfMetrics _metrics;
    private readonly MqttBrokerService _brokerService;
    private readonly MqttPublisherService _publisherService;
    private readonly MqttSubscriberService _subscriberService;
    private readonly DispatcherTimer _timer;

    private long _lastFrames;
    private long _lastBytes;
    private DateTime _lastTime;

    [ObservableProperty] private int _targetFps = 30;
    [ObservableProperty] private int _payloadSizeMb = 64;
    [ObservableProperty] private int _chunkSizeMb = 4;
    [ObservableProperty] private int _qos = 0;
    
    // Advanced Tuning Options
    [ObservableProperty] private bool _tcpNoDelay = true;
    [ObservableProperty] private int _socketBufferSizeMb = 16;
    [ObservableProperty] private bool _parallelChunkPublish = false;
    [ObservableProperty] private int _maxPendingMessages = 1000;
    
    [ObservableProperty] private double _currentFps;
    [ObservableProperty] private double _currentBandwidthMb;
    [ObservableProperty] private double _averageLatencyMs;
    [ObservableProperty] private bool _isRunning;

    public MainViewModel()
    {
        _metrics = new PerfMetrics();
        _brokerService = new MqttBrokerService();
        _publisherService = new MqttPublisherService(_metrics);
        _subscriberService = new MqttSubscriberService(_metrics);

        _timer = new DispatcherTimer
        {
            Interval = TimeSpan.FromSeconds(1)
        };
        _timer.Tick += OnTimerTick;
    }

    private void OnTimerTick(object? sender, EventArgs e)
    {
        var now = DateTime.UtcNow;
        var elapsedSeconds = (now - _lastTime).TotalSeconds;
        
        long currentFrames = _metrics.FramesTransferred;
        long currentBytes = _metrics.BytesTransferred;

        if (elapsedSeconds > 0)
        {
            CurrentFps = (currentFrames - _lastFrames) / elapsedSeconds;
            
            // Bandwidth in MB/s
            double bytesPerSec = (currentBytes - _lastBytes) / elapsedSeconds;
            CurrentBandwidthMb = bytesPerSec / (1024 * 1024);
        }

        AverageLatencyMs = _metrics.AverageLatencyMs;

        _lastFrames = currentFrames;
        _lastBytes = currentBytes;
        _lastTime = now;
    }

    [RelayCommand]
    private async Task StartTestAsync()
    {
        if (IsRunning) return;

        // 1. Start Broker
        await _brokerService.StartAsync(1883, MaxPendingMessages);

        // 2. Start Subscriber
        await _subscriberService.ConnectAsync("127.0.0.1", 1883, TcpNoDelay, SocketBufferSizeMb * 1024 * 1024);

        // 3. Start Publisher
        await _publisherService.ConnectAsync("127.0.0.1", 1883, TcpNoDelay, SocketBufferSizeMb * 1024 * 1024);

        // Reset metrics
        _metrics.Reset();
        _lastFrames = 0;
        _lastBytes = 0;
        _lastTime = DateTime.UtcNow;

        _publisherService.StartPublishing(PayloadSizeMb, TargetFps, ChunkSizeMb, Qos, ParallelChunkPublish);

        IsRunning = true;
        _timer.Start();
    }

    [RelayCommand]
    private async Task StopTestAsync()
    {
        if (!IsRunning) return;

        _timer.Stop();
        
        _publisherService.StopPublishing();
        await _publisherService.DisconnectAsync();
        await _subscriberService.DisconnectAsync();
        await _brokerService.StopAsync();

        IsRunning = false;
        CurrentFps = 0;
        CurrentBandwidthMb = 0;
        AverageLatencyMs = 0;
    }
}

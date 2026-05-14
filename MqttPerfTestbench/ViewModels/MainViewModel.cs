using System;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using MqttPerfTestbench.Models;
using MqttPerfTestbench.Models.Compressors;
using MqttPerfTestbench.Models.Interfaces;
using MqttPerfTestbench.Models.Predictors;
using MqttPerfTestbench.Services.Grpc;
using MqttPerfTestbench.Services.Mqtt;
using MqttPerfTestbench.Services.Tcp;
using MqttPerfTestbench.Services.Zmq;
using MqttPerfTestbench.Services;
using MqttPerfTestbench.Services.H265;

namespace MqttPerfTestbench.ViewModels;

public partial class MainViewModel : ObservableObject
{
    private readonly PerfMetrics _metrics;
    private readonly MqttBrokerService _brokerService;
    private readonly DispatcherTimer _timer;

    private ITransportPublisher? _publisher;
    private ITransportSubscriber? _subscriber;

    private long _lastFrames;
    private long _lastBytes;
    private DateTime _lastTime;

    // Collections
    public ObservableCollection<string> Protocols { get; } = new(new[] { "MQTT", "ZMQ", "gRPC", "TCP", "UDP", "H.265" });
    public ObservableCollection<IDqcmPredictor> Predictors { get; } = new(new IDqcmPredictor[] { new DqcmNonePredictor(), new DqcmLeftPredictor(), new DqcmTopPredictor() });
    public ObservableCollection<IBlockCompressor> Compressors { get; } = new(new IBlockCompressor[] { new NoneCompressor(), new Lz4Compressor(), new ZstdCompressor(), new LzwCompressor() });

    // Selections
    [ObservableProperty] private string _selectedProtocol = "MQTT";
    [ObservableProperty] private IDqcmPredictor _selectedPredictor;
    [ObservableProperty] private IBlockCompressor _selectedCompressor;

    // Basic Config
    [ObservableProperty] private int _targetFps = 30;
    [ObservableProperty] private int _payloadSizeMb = 64;
    [ObservableProperty] private int _imageWidth = 8192;
    [ObservableProperty] private int _imageHeight = 8192;
    [ObservableProperty] private int _chunkSizeMb = 4;
    [ObservableProperty] private int _compressionLevel = 3;
    
    // Advanced Tuning Options
    [ObservableProperty] private bool _tcpNoDelay = true;
    [ObservableProperty] private int _socketBufferSizeMb = 16;
    [ObservableProperty] private bool _parallelChunkPublish = false;
    [ObservableProperty] private int _maxPendingMessages = 1000;
    [ObservableProperty] private int _qos = 0;
    
    // Metrics
    [ObservableProperty] private double _currentFps;
    [ObservableProperty] private double _currentBandwidthMb;
    [ObservableProperty] private double _averageLatencyMs;
    [ObservableProperty] private double _predictionTimeMs;
    [ObservableProperty] private double _compressionTimeMs;
    [ObservableProperty] private double _compressedSizeMb;
    
    [ObservableProperty] private bool _isRunning;

    public MainViewModel()
    {
        _metrics = new PerfMetrics();
        _brokerService = new MqttBrokerService();

        _selectedPredictor = Predictors[0];
        _selectedCompressor = Compressors[0];

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
            // FPS = (현재까지 전송된 총 프레임 수 - 마지막 측정 시점의 총 프레임 수) / 경과 시간(초)
            CurrentFps = (currentFrames - _lastFrames) / elapsedSeconds;
            
            // 대역폭(MB/s) 계산:
            // 1. (현재까지 전송된 총 바이트 수 - 마지막 측정 시점의 총 바이트 수) = 해당 기간 동안 전송된 바이트
            // 2. 위 값을 경과 시간(초)으로 나누어 초당 바이트(Bytes/s)를 구함
            // 3. 1024 * 1024 (1 MiB)로 나누어 MB/s 단위로 변환
            double bytesPerSec = (currentBytes - _lastBytes) / elapsedSeconds;
            CurrentBandwidthMb = bytesPerSec / (1024 * 1024);
        }

        AverageLatencyMs = _metrics.AverageLatencyMs;

        _lastFrames = currentFrames;
        _lastBytes = currentBytes;
        _lastTime = now;
    }

    private void SetupTransport()
    {
        switch (SelectedProtocol)
        {
            case "MQTT":
                _publisher = new MqttTransportPublisher();
                _subscriber = new MqttTransportSubscriber(_metrics);
                break;
            case "ZMQ":
                _publisher = new ZmqTransportPublisher();
                _subscriber = new ZmqTransportSubscriber(_metrics);
                break;
            case "gRPC":
                _publisher = new GrpcTransportPublisher();
                _subscriber = new GrpcTransportSubscriber(_metrics);
                break;
            case "TCP":
                _publisher = new TcpTransportPublisher();
                _subscriber = new TcpTransportSubscriber(_metrics);
                break;
            case "UDP":
                _publisher = new Services.Udp.UdpTransportPublisher();
                _subscriber = new Services.Udp.UdpTransportSubscriber(_metrics);
                break;
            case "H.265":
                _publisher = new H265TransportPublisher();
                _subscriber = new H265TransportSubscriber(_metrics);
                break;
        }
    }

    [RelayCommand]
    private async Task StartTestAsync()
    {
        if (IsRunning) return;
        
        // gRPC unencrypted support for HTTP/2
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        
        IsRunning = true; // prevent double click

        try
        {
            SetupTransport();

            var options = new TransportOptions
            {
                Server = "127.0.0.1",
                Port = SelectedProtocol switch {
                    "gRPC" => 50051,
                    "H.265" => 9000,
                    _ => 1883
                },
                Qos = Qos,
                HighWatermark = MaxPendingMessages,
                MaxMessageSizeMb = 100,
                TcpNoDelay = TcpNoDelay,
                BufferSizeMb = SocketBufferSizeMb,
                ParallelChunkPublish = ParallelChunkPublish,
                ChunkSizeMb = ChunkSizeMb,
                Width = ImageWidth,
                Height = ImageHeight,
                UseGpu = false, // Set true manually for NVENC
                Crf = 28,
                FfmpegPath = OperatingSystem.IsMacOS() ? "/opt/homebrew/bin/ffmpeg" : "ffmpeg" 
            };

            // Start MQTT broker if needed
            if (SelectedProtocol == "MQTT")
            {
                await _brokerService.StartAsync(options.Port, MaxPendingMessages);
            }

            // Pipeline: 1. Generate Raw Data
            int payloadBytes = SelectedProtocol == "H.265" 
                ? ImageWidth * ImageHeight * 4 
                : PayloadSizeMb * 1024 * 1024;
            byte[] rawData = MemoryBufferPool.Rent(payloadBytes);
            Array.Fill(rawData, (byte)128); // dummy gray image

            // Pipeline: 2. Prediction
            var sw = Stopwatch.StartNew();
            SelectedPredictor.ApplyPrediction(rawData.AsSpan(0, payloadBytes), ImageWidth, ImageHeight);
            PredictionTimeMs = sw.Elapsed.TotalMilliseconds;

            // Pipeline: 3. Compression
            sw.Restart();
            byte[] compressedData = SelectedCompressor.Compress(rawData.AsSpan(0, payloadBytes), CompressionLevel);
            CompressionTimeMs = sw.Elapsed.TotalMilliseconds;
            CompressedSizeMb = (double)compressedData.Length / (1024 * 1024);

            MemoryBufferPool.Return(rawData); // We are done with rawData

            // Pipeline: 4. Transport Setup
            if (SelectedProtocol == "TCP" || SelectedProtocol == "ZMQ")
            {
                // Publisher is the Listener/Server for these protocols in current impl.
                // Subscriber is the Connector/Client.
                // Start Listener first, but Publisher.ConnectAsync for TCP blocks on AcceptAsync.
                var pubConnectTask = _publisher!.ConnectAsync(options);
                
                // Give listener a moment to bind
                await Task.Delay(500); 
                
                await _subscriber!.ConnectAsync(options);
                await pubConnectTask;
            }
            else if (SelectedProtocol == "UDP")
            {
                // UDP Subscriber Binds first
                await _subscriber!.ConnectAsync(options);
                await _publisher!.ConnectAsync(options);
            }
            else if (SelectedProtocol == "H.265")
            {
                // H.265 Publisher is Listener (?listen=1), Subscriber is Connector
                await _publisher!.ConnectAsync(options);
                await Task.Delay(1000); // Wait for FFmpeg to start listening
                await _subscriber!.ConnectAsync(options);
            }
            else // MQTT, gRPC
            {
                if (_subscriber != null) await _subscriber.ConnectAsync(options);
                if (_publisher != null) await _publisher.ConnectAsync(options);
            }

            // Reset metrics
            _metrics.Reset();
            _lastFrames = 0;
            _lastBytes = 0;
            _lastTime = DateTime.UtcNow;

            // Start loop
            _publisher?.StartPublishing(compressedData, TargetFps, options);

            _timer.Start();
        }
        catch (Exception ex)
        {
            IsRunning = false;
            System.Diagnostics.Debug.WriteLine($"Test failed: {ex}");
            // Optional: you can show a message box here if needed.
        }
    }

    [RelayCommand]
    private async Task StopTestAsync()
    {
        if (!IsRunning) return;

        _timer.Stop();
        
        if (_publisher != null)
        {
            _publisher.StopPublishing();
            await _publisher.DisconnectAsync();
        }
        
        if (_subscriber != null)
        {
            await _subscriber.DisconnectAsync();
        }

        if (SelectedProtocol == "MQTT")
        {
            await _brokerService.StopAsync();
        }

        IsRunning = false;
        CurrentFps = 0;
        CurrentBandwidthMb = 0;
        AverageLatencyMs = 0;
    }
}

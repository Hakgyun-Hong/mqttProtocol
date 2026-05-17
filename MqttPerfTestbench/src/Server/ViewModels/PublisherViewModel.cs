using System;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Compressors;
using MqttPerfTestbench.Core.Models.Interfaces;
using MqttPerfTestbench.Core.Models.Predictors;
using MqttPerfTestbench.Core.Services;
using MqttPerfTestbench.Core.Services.Mqtt;
using MqttPerfTestbench.Core.Services.Tcp;
using MqttPerfTestbench.Core.Services.Zmq;
using MqttPerfTestbench.Core.Services.H264;
using MqttPerfTestbench.Core.Services.H265;
using MqttPerfTestbench.Core.Services.Udp;
using MqttPerfTestbench.Core.Services.Grpc;

namespace MqttPerfTestbench.Server.ViewModels;

public partial class PublisherViewModel : ObservableObject
{
    // ─── 의존 서비스 ─────────────────────────────────────────────────────────
    private readonly MqttBrokerService _brokerService = new();
    private ITransportPublisher? _publisher;
    private byte[]? _preparedPayload;

    // ─── 컬렉션 ──────────────────────────────────────────────────────────────
    public ObservableCollection<string> Protocols { get; } =
        new(new[] { "MQTT", "ZMQ", "TCP", "UDP", "H.264", "H.265", "gRPC" });

    public ObservableCollection<IDqcmPredictor> Predictors { get; } = new(new IDqcmPredictor[]
        { new DqcmNonePredictor(), new DqcmLeftPredictor(), new DqcmTopPredictor() });

    public ObservableCollection<IBlockCompressor> Compressors { get; } = new(new IBlockCompressor[]
        { new NoneCompressor(), new Lz4Compressor(), new ZstdCompressor(), new LzwCompressor() });

    /// <summary>현재 연결된 클라이언트 목록</summary>
    public ObservableCollection<string> ConnectedClients { get; } = new();

    // ─── 바인딩 프로퍼티 ─────────────────────────────────────────────────────
    [ObservableProperty] private string _selectedProtocol = "TCP";
    [ObservableProperty] private IDqcmPredictor _selectedPredictor;
    [ObservableProperty] private IBlockCompressor _selectedCompressor;

    [ObservableProperty] private int  _port       = ProtocolPorts.Get("TCP");
    [ObservableProperty] private int  _targetFps  = 30;
    [ObservableProperty] private int  _imageWidth  = 8192;
    [ObservableProperty] private int  _imageHeight = 8192;
    [ObservableProperty] private bool _useGpu      = false;
    [ObservableProperty] private int  _crf         = 28;

    // MQTT 전용
    [ObservableProperty] private bool _useExternalBroker = false;
    [ObservableProperty] private string _externalBrokerHost = "127.0.0.1";

    // 상태
    [ObservableProperty] private bool _isServerOpen;
    [ObservableProperty] private bool _isPublishing;
    [ObservableProperty] private string _serverStatus = "CLOSED";

    // 통계
    [ObservableProperty] private double _predictionTimeMs;
    [ObservableProperty] private double _compressionTimeMs;
    [ObservableProperty] private double _compressedSizeMb;

    public PublisherViewModel()
    {
        _selectedPredictor  = Predictors[0];
        _selectedCompressor = Compressors[0];
    }

    // 프로토콜 변경 시 포트 자동 갱신
    partial void OnSelectedProtocolChanged(string value)
    {
        Port = ProtocolPorts.Get(value);
        OnPropertyChanged(nameof(IsMqttProtocol));
        OnPropertyChanged(nameof(ShowExternalBrokerHost));
    }

    partial void OnUseExternalBrokerChanged(bool value) =>
        OnPropertyChanged(nameof(ShowExternalBrokerHost));

    public bool IsMqttProtocol => SelectedProtocol == "MQTT";
    public bool ShowExternalBrokerHost => IsMqttProtocol && UseExternalBroker;

    // ─── 서버 오픈 ───────────────────────────────────────────────────────────
    [RelayCommand]
    private async Task OpenServerAsync()
    {
        if (IsServerOpen) return;
        ServerStatus = "OPENING...";

        try
        {
            SetupPublisher();

            var opts = GetOptions();

            // MQTT: 내장 또는 외부 브로커
            if (SelectedProtocol == "MQTT")
            {
                _brokerService.ClientConnected    += OnClientConnected;
                _brokerService.ClientDisconnected += OnClientDisconnected;
                await _brokerService.StartAsync(opts.Port, useExternal: UseExternalBroker);
            }

            // Publisher의 연결 이벤트 구독
            _publisher!.ClientConnected    += OnClientConnected;
            _publisher!.ClientDisconnected += OnClientDisconnected;

            await _publisher.OpenAsync(opts);

            IsServerOpen = true;
            ServerStatus = "LISTENING";
        }
        catch (Exception ex)
        {
            ServerStatus = "CLOSED";
            Console.WriteLine($"[Server] Open failed: {ex.Message}");
        }
    }

    // ─── 서버 닫기 ───────────────────────────────────────────────────────────
    [RelayCommand]
    private async Task CloseServerAsync()
    {
        if (!IsServerOpen) return;

        await StopAndCleanup();
        ServerStatus = "CLOSED";
    }

    // ─── 발행 시작 ───────────────────────────────────────────────────────────
    [RelayCommand]
    private void StartPublishing()
    {
        if (!IsServerOpen || IsPublishing) return;

        // 데이터 준비
        int payloadBytes = IsVideoProtocol ? ImageWidth * ImageHeight : 64 * 1024 * 1024;
        byte[] rawData = new byte[payloadBytes];
        Array.Fill(rawData, (byte)128);

        var sw = Stopwatch.StartNew();
        if (IsVideoProtocol)
        {
            PredictionTimeMs = 0;
            CompressionTimeMs = 0;
            _preparedPayload = rawData;
            CompressedSizeMb = (double)_preparedPayload.Length / (1024 * 1024);
        }
        else
        {
            SelectedPredictor.ApplyPrediction(rawData, ImageWidth, ImageHeight);
            PredictionTimeMs = sw.Elapsed.TotalMilliseconds;

            sw.Restart();
            _preparedPayload = SelectedCompressor.Compress(rawData, 3);
            CompressionTimeMs = sw.Elapsed.TotalMilliseconds;
            CompressedSizeMb  = (double)_preparedPayload.Length / (1024 * 1024);
        }

        _publisher?.StartPublishing(_preparedPayload, TargetFps, GetOptions());
        IsPublishing = true;
        ServerStatus = "PUBLISHING";
    }

    // ─── 발행 중지 ───────────────────────────────────────────────────────────
    [RelayCommand]
    private void StopPublishing()
    {
        if (!IsPublishing) return;
        _publisher?.StopPublishing();
        IsPublishing = false;
        ServerStatus = "LISTENING";
    }

    // ─── 내부 헬퍼 ──────────────────────────────────────────────────────────

    private bool IsVideoProtocol =>
        SelectedProtocol is "H.264" or "H.265";

    private TransportOptions GetOptions() => new()
    {
        Server           = UseExternalBroker ? ExternalBrokerHost : "0.0.0.0",
        Port             = Port,
        Width            = ImageWidth,
        Height           = ImageHeight,
        UseGpu           = UseGpu,
        Crf              = Crf,
        UseExternalBroker = UseExternalBroker,
        FfmpegPath       = OperatingSystem.IsMacOS() ? "/opt/homebrew/bin/ffmpeg" : "ffmpeg"
    };

    private void SetupPublisher()
    {
        _publisher?.Dispose();
        _publisher = SelectedProtocol switch
        {
            "MQTT"  => new MqttTransportPublisher(),
            "ZMQ"   => new ZmqTransportPublisher(),
            "TCP"   => new TcpTransportPublisher(),
            "UDP"   => new UdpTransportPublisher(),
            "H.264" => new H264TransportPublisher(),
            "H.265" => new H265TransportPublisher(),
            "gRPC"  => new GrpcTransportPublisher(),
            _       => throw new NotSupportedException($"Unknown protocol: {SelectedProtocol}")
        };
    }

    private async Task StopAndCleanup()
    {
        if (IsPublishing) StopPublishing();
        if (_publisher != null)
        {
            _publisher.ClientConnected    -= OnClientConnected;
            _publisher.ClientDisconnected -= OnClientDisconnected;
            await _publisher.CloseAsync();
            _publisher.Dispose();
            _publisher = null;
        }
        if (SelectedProtocol == "MQTT")
        {
            _brokerService.ClientConnected    -= OnClientConnected;
            _brokerService.ClientDisconnected -= OnClientDisconnected;
            await _brokerService.StopAsync();
        }
        IsServerOpen = false;
        Dispatcher.UIThread.Post(() => ConnectedClients.Clear());
    }

    private void OnClientConnected(string ep) =>
        Dispatcher.UIThread.Post(() => ConnectedClients.Add(ep));

    private void OnClientDisconnected(string ep) =>
        Dispatcher.UIThread.Post(() => ConnectedClients.Remove(ep));
}

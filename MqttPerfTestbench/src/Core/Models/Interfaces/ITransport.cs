using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MqttPerfTestbench.Core.Models.Interfaces;

/// <summary>프로토콜별 기본 포트 매핑</summary>
public static class ProtocolPorts
{
    public static readonly Dictionary<string, int> Default = new()
    {
        ["MQTT"]  = 1883,
        ["ZMQ"]   = 5555,
        ["TCP"]   = 7002,
        ["UDP"]   = 7001,
        ["H.264"] = 9001,
        ["H.265"] = 9000,
        ["gRPC"]  = 50051,
    };

    public static int Get(string protocol) =>
        Default.TryGetValue(protocol, out var port) ? port : 1883;
}

public class TransportOptions
{
    public string Server   { get; set; } = "127.0.0.1";
    public int    Port     { get; set; } = 1883;

    // MQTT
    public int  Qos              { get; set; } = 0;
    public bool UseExternalBroker { get; set; } = false;   // true → 외부 Mosquitto 등

    // ZMQ
    public int HighWatermark { get; set; } = 1000;

    // TCP/General
    public bool TcpNoDelay    { get; set; } = true;
    public int  BufferSizeMb  { get; set; } = 16;

    // Chunking
    public bool ParallelChunkPublish { get; set; } = false;
    public int  ChunkSizeMb          { get; set; } = 4;

    // H.264 / H.265
    public int    Width      { get; set; } = 8192;
    public int    Height     { get; set; } = 8192;
    public bool   UseGpu     { get; set; } = false;
    public int    Crf        { get; set; } = 28;
    public string FfmpegPath { get; set; } = "ffmpeg";
}

public interface ITransportPublisher : IDisposable
{
    /// <summary>클라이언트가 연결되었을 때 발생 (IP:Port 문자열)</summary>
    event Action<string>? ClientConnected;

    /// <summary>클라이언트가 끊어졌을 때 발생 (IP:Port 문자열)</summary>
    event Action<string>? ClientDisconnected;

    /// <summary>서버 포트를 열고 연결을 대기합니다.</summary>
    Task OpenAsync(TransportOptions options);

    /// <summary>서버 포트를 닫습니다.</summary>
    Task CloseAsync();

    /// <summary>연속 전송 루프를 시작합니다.</summary>
    void StartPublishing(byte[] payload, int targetFps, TransportOptions options);

    /// <summary>전송 루프를 중지합니다.</summary>
    void StopPublishing();
}

public interface ITransportSubscriber : IDisposable
{
    /// <summary>서버에 연결합니다.</summary>
    Task ConnectAsync(TransportOptions options);

    /// <summary>연결을 해제합니다.</summary>
    Task DisconnectAsync();
}

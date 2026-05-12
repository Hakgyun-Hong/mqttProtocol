using System;
using System.Threading;
using System.Threading.Tasks;

namespace MqttPerfTestbench.Models.Interfaces;

public class TransportOptions
{
    public string Server { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 1883;
    
    // MQTT Specific
    public int Qos { get; set; } = 0;
    
    // ZMQ Specific
    public int HighWatermark { get; set; } = 1000;
    
    // gRPC Specific
    public int MaxMessageSizeMb { get; set; } = 100;
    
    // TCP/General Tuning
    public bool TcpNoDelay { get; set; } = true;
    public int BufferSizeMb { get; set; } = 16;
    
    // Chunking
    public bool ParallelChunkPublish { get; set; } = false;
    public int ChunkSizeMb { get; set; } = 4;
}

public interface ITransportPublisher
{
    Task ConnectAsync(TransportOptions options);
    Task DisconnectAsync();
    
    /// <summary>
    /// Starts the continuous publish loop using the provided pre-processed (predicted & compressed) payload.
    /// </summary>
    void StartPublishing(byte[] compressedPayload, int targetFps, TransportOptions options);
    void StopPublishing();
}

public interface ITransportSubscriber
{
    Task ConnectAsync(TransportOptions options);
    Task DisconnectAsync();
}

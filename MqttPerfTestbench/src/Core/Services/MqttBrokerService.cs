using System;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Server;

namespace MqttPerfTestbench.Core.Services;

public class MqttBrokerService
{
    private MqttServer? _mqttServer;

    /// <summary>클라이언트가 연결되었을 때 발생 (ClientId)</summary>
    public event Action<string>? ClientConnected;
    /// <summary>클라이언트가 끊어졌을 때 발생 (ClientId)</summary>
    public event Action<string>? ClientDisconnected;

    /// <summary>
    /// 내장 브로커를 시작합니다.
    /// UseExternalBroker=true 이면 이 메서드는 아무것도 하지 않습니다 (외부 Mosquitto 등을 사용).
    /// </summary>
    public async Task StartAsync(int port = 1883, int maxPendingMessages = 1000, bool useExternal = false)
    {
        if (useExternal || _mqttServer != null) return;

        var mqttFactory = new MqttFactory();
        var options = new MqttServerOptionsBuilder()
            .WithDefaultEndpoint()
            .WithDefaultEndpointPort(port)
            .WithDefaultCommunicationTimeout(TimeSpan.FromSeconds(15))
            .Build();

        _mqttServer = mqttFactory.CreateMqttServer(options);

        _mqttServer.ClientConnectedAsync += e =>
        {
            ClientConnected?.Invoke(e.ClientId);
            return Task.CompletedTask;
        };

        _mqttServer.ClientDisconnectedAsync += e =>
        {
            ClientDisconnected?.Invoke(e.ClientId);
            return Task.CompletedTask;
        };

        await _mqttServer.StartAsync();
        Console.WriteLine($"[MQTT Broker] Started on port {port}");
    }

    public async Task StopAsync()
    {
        if (_mqttServer != null)
        {
            await _mqttServer.StopAsync();
            _mqttServer.Dispose();
            _mqttServer = null;
        }
    }
}

using System;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Server;

namespace MqttPerfTestbench.Services;

public class MqttBrokerService
{
    private MqttServer? _mqttServer;

    public async Task StartAsync(int port = 1883, int maxPendingMessages = 1000)
    {
        if (_mqttServer != null) return;

        var mqttFactory = new MqttFactory();
        var mqttServerOptions = new MqttServerOptionsBuilder()
            .WithDefaultEndpoint()
            .WithDefaultEndpointPort(port)
            // Limit the internal queue to avoid OutOfMemory on fast publisher, slow subscriber
            .WithDefaultCommunicationTimeout(TimeSpan.FromSeconds(15))
            .Build();

        _mqttServer = mqttFactory.CreateMqttServer(mqttServerOptions);
        await _mqttServer.StartAsync();
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

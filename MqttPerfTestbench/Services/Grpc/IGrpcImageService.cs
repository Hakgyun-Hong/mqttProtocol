using System.ServiceModel;
using System.Threading.Tasks;

namespace MqttPerfTestbench.Services.Grpc;

[ServiceContract]
public interface IGrpcImageService
{
    [OperationContract]
    Task SendChunkAsync(byte[] payload);
}

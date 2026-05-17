using System;
using System.Collections.Generic;
using System.ServiceModel;
using System.Threading;
using ProtoBuf;

namespace MqttPerfTestbench.Core.Services.Grpc;

[ServiceContract]
public interface IGrpcImageService
{
    [OperationContract]
    IAsyncEnumerable<ImageFrame> SubscribeFramesAsync(CancellationToken cancellationToken = default);
}

[ProtoContract]
public class ImageFrame
{
    [ProtoMember(1)]
    public byte[] Payload { get; set; } = Array.Empty<byte>();
}

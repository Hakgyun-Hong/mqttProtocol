using System;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Models.Predictors;

public class DqcmLeftPredictor : IDqcmPredictor
{
    public string Name => "1D-Left";

    public void ApplyPrediction(Span<byte> buffer, int width, int height)
    {
        if (buffer.Length == 0) return;
        
        // Reverse iteration to allow in-place modification
        for (int i = buffer.Length - 1; i > 0; i--)
        {
            buffer[i] = (byte)(buffer[i] - buffer[i - 1]);
        }
    }

    public void RestorePrediction(Span<byte> buffer, int width, int height)
    {
        if (buffer.Length == 0) return;
        
        // Forward iteration for restoration
        for (int i = 1; i < buffer.Length; i++)
        {
            buffer[i] = (byte)(buffer[i] + buffer[i - 1]);
        }
    }
}

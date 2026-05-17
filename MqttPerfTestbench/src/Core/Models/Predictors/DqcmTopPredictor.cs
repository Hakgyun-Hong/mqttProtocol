using System;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Models.Predictors;

public class DqcmTopPredictor : IDqcmPredictor
{
    public string Name => "2D-Top";

    public void ApplyPrediction(Span<byte> buffer, int width, int height)
    {
        if (buffer.Length <= width || width <= 0) return;

        // Reverse iteration for in-place
        for (int i = buffer.Length - 1; i >= width; i--)
        {
            buffer[i] = (byte)(buffer[i] - buffer[i - width]);
        }
    }

    public void RestorePrediction(Span<byte> buffer, int width, int height)
    {
        if (buffer.Length <= width || width <= 0) return;

        // Forward iteration for restoration
        for (int i = width; i < buffer.Length; i++)
        {
            buffer[i] = (byte)(buffer[i] + buffer[i - width]);
        }
    }
}

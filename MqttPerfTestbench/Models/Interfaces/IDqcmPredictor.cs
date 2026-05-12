using System;

namespace MqttPerfTestbench.Models.Interfaces;

public interface IDqcmPredictor
{
    string Name { get; }
    
    /// <summary>
    /// Applies delta prediction in-place or returns a predicted buffer.
    /// Requires Image Width and Height for 2D predictors.
    /// </summary>
    void ApplyPrediction(Span<byte> buffer, int width, int height);
    
    void RestorePrediction(Span<byte> buffer, int width, int height);
}

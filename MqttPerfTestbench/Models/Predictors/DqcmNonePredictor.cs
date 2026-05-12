using System;
using MqttPerfTestbench.Models.Interfaces;

namespace MqttPerfTestbench.Models.Predictors;

public class DqcmNonePredictor : IDqcmPredictor
{
    public string Name => "None";

    public void ApplyPrediction(Span<byte> buffer, int width, int height)
    {
        // Do nothing
    }

    public void RestorePrediction(Span<byte> buffer, int width, int height)
    {
        // Do nothing
    }
}

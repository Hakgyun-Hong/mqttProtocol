using System;
using MqttPerfTestbench.Core.Models.Interfaces;

namespace MqttPerfTestbench.Core.Models.Predictors;

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

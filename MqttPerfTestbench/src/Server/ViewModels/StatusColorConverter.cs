using System;
using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;

namespace MqttPerfTestbench.Server.ViewModels;

/// <summary>서버 상태 문자열 → 배경색 변환</summary>
public class StatusColorConverter : IValueConverter
{
    public static readonly StatusColorConverter Instance = new();

    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        return (value?.ToString() ?? "") switch
        {
            "PUBLISHING" => new SolidColorBrush(Color.Parse("#1B5E20")),
            "LISTENING"  => new SolidColorBrush(Color.Parse("#1565C0")),
            "OPENING..."  => new SolidColorBrush(Color.Parse("#4E342E")),
            _            => new SolidColorBrush(Color.Parse("#37474F")),
        };
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        => throw new NotImplementedException();
}

using System.Windows;
using System;
using System.Globalization;
using System.Windows.Data;

namespace MqttPerfTestbench.ViewModels
{
    public class InverseBooleanConverter : IValueConverter
    {
        public static readonly InverseBooleanConverter Instance = new InverseBooleanConverter();

        public object? ValidateAndConvert(object? value)
        {
            if (value is bool b)
                return !b;
            return null;
        }

        public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
            => ValidateAndConvert(value);

        public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
            => ValidateAndConvert(value);
    }
}

namespace MqttPerfTestbench
{
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }
    }
}

using Avalonia.Controls;
using MqttPerfTestbench.ViewModels;

namespace MqttPerfTestbench;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        DataContext = new MainViewModel();
    }
}

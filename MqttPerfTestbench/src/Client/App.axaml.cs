using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using MqttPerfTestbench.Client.ViewModels;
using MqttPerfTestbench.Client.Views;

namespace MqttPerfTestbench.Client;

public partial class App : Application
{
    public override void Initialize()
    {
        AvaloniaXamlLoader.Load(this);
    }

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            desktop.MainWindow = new SubscriberWindow
            {
                DataContext = new SubscriberViewModel()
            };
        }

        base.OnFrameworkInitializationCompleted();
    }
}

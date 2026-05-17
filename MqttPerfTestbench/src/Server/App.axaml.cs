using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using MqttPerfTestbench.Server.ViewModels;
using MqttPerfTestbench.Server.Views;

namespace MqttPerfTestbench.Server;

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
            desktop.MainWindow = new PublisherWindow
            {
                DataContext = new PublisherViewModel()
            };
        }

        base.OnFrameworkInitializationCompleted();
    }
}

using System;

namespace MqttPerfTestbench.Core.Services;

/// <summary>
/// FFmpeg 네이티브 라이브러리 연동 검증용 초기화 헬퍼.
/// C# P/Invoke의 런타임 로드가 정상 동작하는지 테스트합니다.
/// </summary>
public static class FFmpegLoader
{
    private static readonly object Lock = new();
    private static bool _initialized;

    public static void Initialize()
    {
        lock (Lock)
        {
            if (_initialized) return;

            try
            {
                // 버전을 조회하여 바인딩이 실제 정상 작동하는지 가볍게 테스트
                unsafe
                {
                    uint version = FFmpegApi.avcodec_version();
                    Console.WriteLine($"[FFmpeg Native] Pure P/Invoke bound successfully! avcodec version: {version}");
                }
                
                _initialized = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FFmpeg Native] Pure P/Invoke binding failed: {ex.Message}");
                throw;
            }
        }
    }
}

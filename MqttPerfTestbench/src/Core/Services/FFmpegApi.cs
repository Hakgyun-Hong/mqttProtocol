using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace MqttPerfTestbench.Core.Services;

/// <summary>
/// [100% Pure C# P/Invoke] FFmpeg Native API Wrapper
/// 별도의 외부 NuGet 패키지 없이 C# 표준 NativeLibrary API와 [DllImport]만 사용하여
/// macOS/Windows의 FFmpeg 네이티브 라이브러리를 동적으로 바인딩합니다.
/// </summary>
public static unsafe class FFmpegApi
{
    private const string AvCodecLib = "avcodec";
    private const string AvUtilLib = "avutil";

    // ─── [코덱 및 픽셀 포맷 표준 상수 정의] ───
    public const int AV_CODEC_ID_H264 = 27;
    public const int AV_CODEC_ID_HEVC = 173; // H.265
    public const int AV_PIX_FMT_YUV420P = 0;

    #region ─── [네이티브 구조체 정의] ───

    [StructLayout(LayoutKind.Sequential)]
    public struct AVRational
    {
        public int num;
        public int den;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct AVCodecContext
    {
        public void* av_class;
        public int log_level_offset;
        public int codec_type; // AVMediaType
        public void* codec; // AVCodec*
        public int codec_id; // AVCodecID
        public uint codec_tag;
        public int bit_rate;
        public int bit_rate_tolerance;
        public int global_quality;
        public int compression_level;
        public int flags;
        public int flags2;
        public byte* extradata;
        public int extradata_size;
        public AVRational time_base;
        public int ticks_per_frame;
        public int delay;
        
        // ─── 중요 해상도 및 영상 정보 (안정적인 오프셋 위치) ───
        public int width;
        public int height;
        public int coded_width;
        public int coded_height;
        public int gop_size;
        public int pix_fmt; // AVPixelFormat
        public int active_thread_type;
        public int thread_type;
        public int thread_count;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct AVFrame
    {
        // YUV420P Plane 포인터 (data_0=Y, data_1=U, data_2=V)
        public byte* data_0;
        public byte* data_1;
        public byte* data_2;
        public byte* data_3;
        public byte* data_4;
        public byte* data_5;
        public byte* data_6;
        public byte* data_7;

        // Plane별 가로 바이트 크기 (Stride)
        public int linesize_0;
        public int linesize_1;
        public int linesize_2;
        public int linesize_3;
        public int linesize_4;
        public int linesize_5;
        public int linesize_6;
        public int linesize_7;

        public byte** extended_data;
        public int width;
        public int height;
        public int nb_samples;
        public int format;
        public int key_frame;
        public int pict_type;
        public AVRational sample_aspect_ratio;
        public long pts;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct AVPacket
    {
        public void* buf;
        public long pts;
        public long dts;
        
        // 원본 압축 데이터 포인터 및 크기
        public byte* data;
        public int size;
        
        public int stream_index;
        public int flags;
    }

    public struct AVCodec { }
    public struct AVDictionary { }

    #endregion

    #region ─── [네이티브 DllImport 메서드 매핑] ───

    // ─── libavutil 함수 ───
    [DllImport(AvUtilLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void* av_malloc(ulong size);

    [DllImport(AvUtilLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void av_free(void* ptr);

    [DllImport(AvUtilLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern int av_dict_set(AVDictionary** pm, string key, string value, int flags);

    // ─── libavcodec 함수 ───
    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern uint avcodec_version();

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern AVCodec* avcodec_find_encoder(int id);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern AVCodec* avcodec_find_encoder_by_name(string name);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern AVCodec* avcodec_find_decoder(int id);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern AVCodecContext* avcodec_alloc_context3(AVCodec* codec);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void avcodec_free_context(AVCodecContext** avctx);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern int avcodec_open2(AVCodecContext* avctx, AVCodec* codec, AVDictionary** options);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern int avcodec_send_frame(AVCodecContext* avctx, AVFrame* frame);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern int avcodec_receive_packet(AVCodecContext* avctx, AVPacket* avpkt);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern int avcodec_send_packet(AVCodecContext* avctx, AVPacket* avpkt);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern int avcodec_receive_frame(AVCodecContext* avctx, AVFrame* frame);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern AVFrame* av_frame_alloc();

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void av_frame_free(AVFrame** frame);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern int av_frame_get_buffer(AVFrame* frame, int align);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern AVPacket* av_packet_alloc();

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void av_packet_free(AVPacket** pkt);

    [DllImport(AvCodecLib, CallingConvention = CallingConvention.Cdecl)]
    public static extern void av_packet_unref(AVPacket* pkt);

    #endregion

    #region ─── [플랫폼 동적 DllImport 리졸버 구현] ───

    static FFmpegApi()
    {
        // .NET Core의 강력한 DllImportResolver를 이용해,
        // macOS Homebrew의 dylib 경로 또는 Windows의 DLL 이름을 런타임에 동적으로 주입 매핑해줍니다.
        NativeLibrary.SetDllImportResolver(Assembly.GetExecutingAssembly(), (libName, assembly, path) =>
        {
            IntPtr handle = IntPtr.Zero;

            if (libName == AvCodecLib)
            {
                if (OperatingSystem.IsMacOS())
                {
                    // macOS Homebrew에 설치된 버전 호환 링크 탐색
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavcodec.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavcodec.61.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavcodec.60.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavcodec.59.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavcodec.58.dylib", out handle)) return handle;
                }
                else if (OperatingSystem.IsWindows())
                {
                    // Windows에 설치된 로컬 DLL 탐색
                    if (NativeLibrary.TryLoad("avcodec-61.dll", out handle)) return handle;
                    if (NativeLibrary.TryLoad("avcodec-60.dll", out handle)) return handle;
                    if (NativeLibrary.TryLoad("avcodec-59.dll", out handle)) return handle;
                    if (NativeLibrary.TryLoad("avcodec-58.dll", out handle)) return handle;
                }
            }

            if (libName == AvUtilLib)
            {
                if (OperatingSystem.IsMacOS())
                {
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavutil.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavutil.59.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavutil.58.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavutil.57.dylib", out handle)) return handle;
                    if (NativeLibrary.TryLoad("/opt/homebrew/lib/libavutil.56.dylib", out handle)) return handle;
                }
                else if (OperatingSystem.IsWindows())
                {
                    if (NativeLibrary.TryLoad("avutil-59.dll", out handle)) return handle;
                    if (NativeLibrary.TryLoad("avutil-58.dll", out handle)) return handle;
                    if (NativeLibrary.TryLoad("avutil-57.dll", out handle)) return handle;
                    if (NativeLibrary.TryLoad("avutil-56.dll", out handle)) return handle;
                }
            }

            return handle;
        });
    }

    #endregion
}

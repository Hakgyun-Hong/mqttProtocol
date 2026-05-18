using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models.Interfaces;
using static MqttPerfTestbench.Core.Services.FFmpegApi;

namespace MqttPerfTestbench.Core.Services.H264;

/// <summary>
/// [In-Process Native] H.264 Publisher
/// 별도의 외부 NuGet 패키지 없이 C# 표준 NativeLibrary API와 [DllImport]만 사용하여
/// C# 메모리 내에서 직접 H.264로 압축 인코딩합니다.
/// 데이터 파이프 전송 비용을 제로(Zero-Copy)화하여 성능 극대화를 이룹니다.
/// </summary>
public class H264TransportPublisher : ITransportPublisher
{
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    // 네이티브 인코딩 관련 포인터 정의 (안전하지 않은 포인터 필드)
    private unsafe AVCodecContext* _codecContext;
    private unsafe AVFrame*        _inputFrame;
    private unsafe AVPacket*       _outputPacket;
    private long            _frameCount;

    // 네트워크 리스너 및 세션
    private TcpListener?    _listener;
    private volatile TcpClient?     _client;
    private volatile NetworkStream? _clientStream;

    private CancellationTokenSource? _openCts;
    private CancellationTokenSource? _publishCts;
    private readonly object _encoderLock = new();

    public async Task OpenAsync(TransportOptions options)
    {
        _openCts = new CancellationTokenSource();
        var token = _openCts.Token;

        // 1. FFmpeg 네이티브 로더 초기화
        FFmpegLoader.Initialize();

        // 2. 인코더 및 하드웨어 가속 세팅
        lock (_encoderLock)
        {
            InitializeEncoder(options);
        }

        // 3. TCP 소켓 리스너 시작 (TCP 포트 바인딩)
        _listener = new TcpListener(IPAddress.Any, options.Port);
        _listener.Start();
        Console.WriteLine($"[H.264 Publisher Native] Listening on port {options.Port}");

        // 4. 클라이언트 수신 루프 실행
        _ = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var newClient = await _listener.AcceptTcpClientAsync(token);
                    var ep = newClient.Client.RemoteEndPoint?.ToString() ?? "unknown";

                    var old = _client;
                    if (old != null)
                    {
                        var oldEp = "unknown";
                        try { oldEp = old.Client.RemoteEndPoint?.ToString() ?? "unknown"; } catch { }
                        _clientStream = null;
                        old.Close();
                        ClientDisconnected?.Invoke(oldEp);
                    }

                    _client = newClient;
                    _clientStream = newClient.GetStream();
                    Console.WriteLine($"[H.264 Native] Client connected: {ep}");
                    ClientConnected?.Invoke(ep);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    if (!token.IsCancellationRequested)
                        Console.WriteLine($"[H.264 Native] Accept error: {ex.Message}");
                }
            }
        }, token);

        await Task.CompletedTask;
    }

    public Task CloseAsync()
    {
        _publishCts?.Cancel();
        _openCts?.Cancel();
        _listener?.Stop();
        
        _clientStream = null;
        _client?.Close();

        lock (_encoderLock)
        {
            ReleaseEncoder();
        }

        _openCts?.Dispose(); _publishCts?.Dispose();
        _openCts = null; _publishCts = null;
        
        return Task.CompletedTask;
    }

    public void StartPublishing(byte[] payload, int targetFps, TransportOptions options)
    {
        StopPublishing();
        _publishCts = new CancellationTokenSource();
        var token = _publishCts.Token;

        Task.Run(async () =>
        {
            double msPerFrame = 1000.0 / targetFps;
            var sw = new Stopwatch();
            var networkHeader = new byte[12]; // [4 bytes: chunk length][8 bytes: timestamp]

            try
            {
                while (!token.IsCancellationRequested)
                {
                    sw.Restart();
                    byte[]? encodedBytes = null;
                    int packetSize = 0;

                    // ─── [네이티브 인코딩 단계 (동기적인 unsafe 블록)] ───
                    lock (_encoderLock)
                    {
                        unsafe
                        {
                            if (_codecContext != null && _inputFrame != null && _outputPacket != null)
                            {
                                fixed (byte* rawPayloadPtr = payload)
                                {
                                    // 1. Grayscale 프레임을 인코더가 받는 YUV420P 형식으로 채우기
                                    // Y plane (밝기값) 복사
                                    for (int y = 0; y < options.Height; y++)
                                    {
                                        byte* src = rawPayloadPtr + (y * options.Width);
                                        byte* dest = _inputFrame->data_0 + (y * _inputFrame->linesize_0);
                                        Buffer.MemoryCopy(src, dest, _inputFrame->linesize_0, options.Width);
                                    }

                                    // U & V plane (색상값)은 무채색 기준인 128로 세팅 (Grayscale 유지)
                                    int uvWidth = options.Width / 2;
                                    int uvHeight = options.Height / 2;
                                    for (int y = 0; y < uvHeight; y++)
                                    {
                                        byte* uDest = _inputFrame->data_1 + (y * _inputFrame->linesize_1);
                                        byte* vDest = _inputFrame->data_2 + (y * _inputFrame->linesize_2);
                                        for (int x = 0; x < uvWidth; x++)
                                        {
                                            uDest[x] = 128;
                                            vDest[x] = 128;
                                        }
                                    }
                                }

                                // 2. PTS 프레임 인덱스 지정 및 인코더에 밀어넣기
                                _inputFrame->pts = _frameCount++;
                                int sendResult = avcodec_send_frame(_codecContext, _inputFrame);
                                if (sendResult >= 0)
                                {
                                    // 3. 인코더로부터 압축 패킷(AVPacket)들 가져오기
                                    if (avcodec_receive_packet(_codecContext, _outputPacket) >= 0)
                                    {
                                        packetSize = _outputPacket->size;
                                        encodedBytes = new byte[packetSize];
                                        Marshal.Copy((IntPtr)_outputPacket->data, encodedBytes, 0, packetSize);
                                        av_packet_unref(_outputPacket);
                                    }
                                }
                                else
                                {
                                    Console.WriteLine($"[H.264 Native] Send frame failed: {sendResult}");
                                }
                            }
                        }
                    }

                    // ─── [네이티브 영역 밖에서 비동기 전송 가능하도록 데이터 발송] ───
                    if (encodedBytes != null && packetSize > 0)
                    {
                        var stream = _clientStream;
                        if (stream != null)
                        {
                            try
                            {
                                // 4. Framed TCP 헤더 채우기 [4 bytes: length][8 bytes: timestamp]
                                BitConverter.TryWriteBytes(networkHeader.AsSpan(0, 4), packetSize);
                                BitConverter.TryWriteBytes(networkHeader.AsSpan(4, 8), Stopwatch.GetTimestamp());

                                // 5. 소켓으로 전송
                                stream.Write(networkHeader, 0, 12);
                                stream.Write(encodedBytes, 0, packetSize);
                                stream.Flush();
                            }
                            catch
                            {
                                // 클라이언트 연결 끊김 감지
                            }
                        }
                    }

                    // Target FPS 속도 조절
                    int delay = (int)(msPerFrame - sw.ElapsedMilliseconds);
                    if (delay > 0) await Task.Delay(delay, token);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine($"[H.264 Native] Publish loop error: {ex.Message}"); }
        }, token);
    }

    public void StopPublishing()
    {
        _publishCts?.Cancel();
        _publishCts?.Dispose();
        _publishCts = null;
    }

    public void Dispose() => CloseAsync().GetAwaiter().GetResult();

    private void InitializeEncoder(TransportOptions options)
    {
        unsafe
        {
            // 1. 적절한 H.264 인코더 찾기 (GPU 가속 혹은 기본 CPU)
            string codecName = options.UseGpu ? (OperatingSystem.IsMacOS() ? "h264_videotoolbox" : "h264_nvenc") : "libx264";
            AVCodec* codec = avcodec_find_encoder_by_name(codecName);
            if (codec == null)
            {
                Console.WriteLine($"[H.264 Native] Warning: Custom codec '{codecName}' not found. Falling back to default CPU H.264.");
                codec = avcodec_find_encoder(AV_CODEC_ID_H264);
            }

            if (codec == null)
                throw new Exception("H.264 encoder could not be resolved.");

            // 2. 인코더 컨텍스트 생성 및 해상도/설정 연결
            _codecContext = avcodec_alloc_context3(codec);
            _codecContext->width = options.Width;
            _codecContext->height = options.Height;
            _codecContext->pix_fmt = AV_PIX_FMT_YUV420P;
            _codecContext->time_base = new AVRational { num = 1, den = 30 };
            
            // 초저지연을 위해 GOP 크기를 1로 고정하여 모든 프레임을 즉시 디코딩할 수 있는 Keyframe으로 발행
            _codecContext->gop_size = 1;

            // 3. 인코딩 튜닝 매개변수 적용 (Preset = Ultrafast, Tune = Zerolatency, CRF)
            AVDictionary* dict = null;
            av_dict_set(&dict, "preset", "ultrafast", 0);
            av_dict_set(&dict, "tune", "zerolatency", 0);
            av_dict_set(&dict, "crf", options.Crf.ToString(), 0);

            int openResult = avcodec_open2(_codecContext, codec, &dict);
            if (openResult < 0)
                throw new Exception($"Failed to open native H.264 codec context: {openResult}");

            // 4. 인코더 입력용 AVFrame 프레임 버퍼 생성
            _inputFrame = av_frame_alloc();
            _inputFrame->format = AV_PIX_FMT_YUV420P;
            _inputFrame->width = options.Width;
            _inputFrame->height = options.Height;
            av_frame_get_buffer(_inputFrame, 32);

            // 5. 인코더 출력 패킷 홀더 생성
            _outputPacket = av_packet_alloc();
            _frameCount = 0;

            Console.WriteLine($"[H.264 Native] Encoder successfully initialized in-process (Codec: {codecName})");
        }
    }

    private void ReleaseEncoder()
    {
        unsafe
        {
            if (_codecContext != null)
            {
                var ctx = _codecContext;
                avcodec_free_context(&ctx);
                _codecContext = null;
            }
            if (_inputFrame != null)
            {
                var f = _inputFrame;
                av_frame_free(&f);
                _inputFrame = null;
            }
            if (_outputPacket != null)
            {
                var p = _outputPacket;
                av_packet_free(&p);
                _outputPacket = null;
            }
        }
    }
}

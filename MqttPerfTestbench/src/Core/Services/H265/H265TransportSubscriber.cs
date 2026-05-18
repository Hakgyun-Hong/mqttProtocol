using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using MqttPerfTestbench.Core.Models;
using MqttPerfTestbench.Core.Models.Interfaces;
using static MqttPerfTestbench.Core.Services.FFmpegApi;

namespace MqttPerfTestbench.Core.Services.H265;

/// <summary>
/// [In-Process Native] H.265 (HEVC) Subscriber
/// 별도의 외부 NuGet 패키지 없이 C# 표준 NativeLibrary API와 [DllImport]만 사용하여
/// 프로세스 내부 메모리에서 직접 디코딩을 수행합니다.
/// OS 파이프 버퍼가 필요 없어, 디코딩 지연이 극단적으로 적으며 매우 안정적으로 레이턴시 메트릭을 계산합니다.
/// </summary>
public class H265TransportSubscriber : ITransportSubscriber
{
    private readonly PerfMetrics _metrics;
    
    // 네트워크 소켓
    private TcpClient?           _tcpClient;
    private CancellationTokenSource? _cts;

    // 네이티브 디코더 리소스 포인터 (안전하지 않은 포인터 필드)
    private unsafe AVCodecContext*      _codecContext;
    private unsafe AVFrame*             _decodedFrame;
    private unsafe AVPacket*            _inputPacket;
    private readonly object      _decoderLock = new();

    // 레이턴시 계산 동기화용 변수
    private long _latestTimestamp = 0;
    private readonly object _tsLock = new();

    public H265TransportSubscriber(PerfMetrics metrics) => _metrics = metrics;

    public async Task ConnectAsync(TransportOptions options)
    {
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        // 1. FFmpeg 네이티브 로더 초기화
        FFmpegLoader.Initialize();

        // 2. TCP 클라이언트 연결 시도
        _tcpClient = new TcpClient();
        for (int i = 0; i < 10; i++)
        {
            try
            {
                await _tcpClient.ConnectAsync(options.Server, options.Port, token);
                Console.WriteLine($"[H.265 Sub Native] Connected to {options.Server}:{options.Port}");
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[H.265 Sub Native] Connection attempt {i + 1} failed: {ex.Message}");
                if (i == 9) throw;
                await Task.Delay(1000, token);
            }
        }

        // 3. 네이티브 디코더 및 버퍼 메모리 할당
        lock (_decoderLock)
        {
            InitializeDecoder();
        }

        // 4. 소켓 수신 및 디코딩 루프 개시
        _ = Task.Run(async () =>
        {
            var netStream = _tcpClient.GetStream();
            var networkHeader = new byte[12]; // [4 bytes: chunk length][8 bytes: timestamp]

            try
            {
                while (!token.IsCancellationRequested)
                {
                    // 1. 12바이트 Framed TCP 헤더 완벽히 읽기
                    if (!await ReadExactAsync(netStream, networkHeader, 12, token)) break;
                    int length = BitConverter.ToInt32(networkHeader, 0);
                    long ts = BitConverter.ToInt64(networkHeader, 4);

                    // 2. 비디오 패킷 원본 바이트 수신
                    var encodedBytes = new byte[length];
                    if (!await ReadExactAsync(netStream, encodedBytes, length, token)) break;

                    // 3. 실시간으로 수신한 패킷의 발행 타임스탬프 갱신
                    lock (_tsLock)
                    {
                        _latestTimestamp = ts;
                    }

                    // ─── [네이티브 디코딩 단계 (동기적인 unsafe 블록)] ───
                    lock (_decoderLock)
                    {
                        unsafe
                        {
                            if (_codecContext != null && _inputPacket != null && _decodedFrame != null)
                            {
                                // 디코더 입력 패킷에 소켓으로 받은 원본 압축 데이터 바인딩
                                byte* nativeBuf = (byte*)av_malloc((ulong)length);
                                Marshal.Copy(encodedBytes, 0, (IntPtr)nativeBuf, length);
                                _inputPacket->data = nativeBuf;
                                _inputPacket->size = length;

                                // 디코더에 압축 비디오 패킷 투입
                                int sendResult = avcodec_send_packet(_codecContext, _inputPacket);
                                av_free(nativeBuf); // 임시 할당된 패킷용 네이티브 메모리 즉각 해제

                                if (sendResult >= 0)
                                {
                                    // 디코더로부터 압축이 풀린 raw 비디오 프레임(AVFrame)을 실시간으로 가져옴
                                    while (avcodec_receive_frame(_codecContext, _decodedFrame) >= 0)
                                    {
                                        int frameWidth = _decodedFrame->width;
                                        int frameHeight = _decodedFrame->height;

                                        // 디코딩 완료된 YUV420P 프레임의 Y Plane(밝기값)에서 8K Grayscale 픽셀 데이터 즉각 복사
                                        byte[] decodedGrayBytes = new byte[frameWidth * frameHeight];
                                        for (int y = 0; y < frameHeight; y++)
                                        {
                                            IntPtr srcPtr = (IntPtr)(_decodedFrame->data_0 + (y * _decodedFrame->linesize_0));
                                            Marshal.Copy(srcPtr, decodedGrayBytes, y * frameWidth, frameWidth);
                                        }

                                        // 4. 고정밀 레이턴시 메트릭스 연산
                                        long arrivalTs;
                                        lock (_tsLock)
                                        {
                                            arrivalTs = _latestTimestamp;
                                        }

                                        if (arrivalTs > 0)
                                        {
                                            long latencyMs = (Stopwatch.GetTimestamp() - arrivalTs) * 1000 / Stopwatch.Frequency;
                                            _metrics.AddFrame(decodedGrayBytes.Length, latencyMs);
                                        }
                                    }
                                }
                                else
                                {
                                    Console.WriteLine($"[H.265 Sub Native] Send packet failed: {sendResult}");
                                }
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine($"[H.265 Sub Native] Loop error: {ex.Message}"); }
        }, token);
    }

    private static async Task<bool> ReadExactAsync(NetworkStream stream, byte[] buffer, int count, CancellationToken token)
    {
        int offset = 0;
        while (offset < count)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), token);
            if (read == 0) return false;
            offset += read;
        }
        return true;
    }

    public Task DisconnectAsync()
    {
        try { _cts?.Cancel(); } catch { }
        try { _cts?.Dispose(); } catch { }
        _cts = null;

        lock (_decoderLock)
        {
            ReleaseDecoder();
        }

        try { _tcpClient?.Close(); } catch { }
        _tcpClient = null;

        return Task.CompletedTask;
    }

    public void Dispose() => DisconnectAsync().GetAwaiter().GetResult();

    private void InitializeDecoder()
    {
        unsafe
        {
            // 1. H.265(HEVC) 디코더 탐색
            AVCodec* codec = avcodec_find_decoder(AV_CODEC_ID_HEVC);
            if (codec == null)
                throw new Exception("H.265 decoder not found in FFmpeg native libraries.");

            // 2. 디코더 컨텍스트 생성 및 연결
            _codecContext = avcodec_alloc_context3(codec);
            
            // 지연 시간 단축을 위해 쓰레드 모델 튜닝 및 Low Delay 속성 플래그 세팅
            // 0x0020은 AV_CODEC_FLAG_LOW_DELAY 플래그 값
            _codecContext->flags |= 0x0020;
            _codecContext->thread_count = 0; // 최적의 CPU 멀티쓰레드 디코딩 자동 조율

            int openResult = avcodec_open2(_codecContext, codec, null);
            if (openResult < 0)
                throw new Exception($"Failed to open native H.265 decoder context: {openResult}");

            // 3. 디코더 출력 프레임 및 수신 패킷 메모리 홀더 생성
            _decodedFrame = av_frame_alloc();
            _inputPacket = av_packet_alloc();

            Console.WriteLine("[H.265 Sub Native] Decoder successfully initialized in-process");
        }
    }

    private void ReleaseDecoder()
    {
        unsafe
        {
            if (_codecContext != null)
            {
                var ctx = _codecContext;
                avcodec_free_context(&ctx);
                _codecContext = null;
            }
            if (_decodedFrame != null)
            {
                var f = _decodedFrame;
                av_frame_free(&f);
                _decodedFrame = null;
            }
            if (_inputPacket != null)
            {
                var p = _inputPacket;
                av_packet_free(&p);
                _inputPacket = null;
            }
        }
    }
}

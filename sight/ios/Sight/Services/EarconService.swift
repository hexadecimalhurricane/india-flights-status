import AVFoundation

final class EarconService {
    private let engine = AVAudioEngine()
    private let env = AVAudioEnvironmentNode()
    private let player = AVAudioPlayerNode()
    private var beatTimer: Timer?

    init() {
        engine.attach(env)
        engine.attach(player)
        engine.connect(player, to: env, format: nil)
        engine.connect(env, to: engine.mainMixerNode, format: nil)
        env.renderingAlgorithm = .HRTFHQ
        try? engine.start()
    }

    /// pan: -1..1, closeness: 0..1
    func setProximity(pan: Float, closeness: Float) {
        beatTimer?.invalidate()
        guard closeness > 0.05 else { return }
        let interval = max(0.09, 0.7 - Double(closeness) * 0.6)
        beatTimer = Timer.scheduledTimer(withTimeInterval: interval, repeats: true) { [weak self] _ in
            self?.tick(pan: pan, closeness: closeness)
        }
    }

    func clear() { beatTimer?.invalidate(); beatTimer = nil }

    private func tick(pan: Float, closeness: Float) {
        let frames: AVAudioFrameCount = 4096
        let sampleRate: Double = 44100
        let format = AVAudioFormat(standardFormatWithSampleRate: sampleRate, channels: 1)!
        let buf = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames)!
        buf.frameLength = frames
        let freq = Double(380 + closeness * 600)
        let ch = buf.floatChannelData![0]
        for i in 0..<Int(frames) {
            let t = Double(i) / sampleRate
            let env = exp(-t * 22)
            ch[i] = Float(sin(2 * .pi * freq * t) * env * 0.4)
        }
        env.position = AVAudio3DPoint(x: pan * 2, y: 0, z: -1)
        player.scheduleBuffer(buf, completionHandler: nil)
        if !player.isPlaying { player.play() }
    }
}

import AVFoundation

enum SpeechPriority: Int { case ambient = 0, narrative = 1, salient = 2, hazard = 3 }

final class SpeechService: NSObject, AVSpeechSynthesizerDelegate {
    private let synth = AVSpeechSynthesizer()
    private var queue: [(String, SpeechPriority)] = []
    private var speakingPriority: Int = -1
    private var recent: [String: Date] = [:]
    private var lastSpoken: String = ""

    override init() {
        super.init()
        synth.delegate = self
        configureAudio()
    }

    private func configureAudio() {
        let s = AVAudioSession.sharedInstance()
        try? s.setCategory(.playback, mode: .spokenAudio, options: [.duckOthers, .mixWithOthers, .allowBluetoothA2DP, .allowAirPlay])
        try? s.setActive(true)
    }

    func say(_ text: String, priority: SpeechPriority, cooldown: TimeInterval = 4) {
        guard !text.isEmpty else { return }
        let now = Date()
        if let last = recent[text], now.timeIntervalSince(last) < cooldown { return }
        recent[text] = now

        if priority.rawValue > speakingPriority {
            synth.stopSpeaking(at: .immediate)
            queue.removeAll()
            queue.append((text, priority))
            drain()
        } else {
            queue.append((text, priority))
            if !synth.isSpeaking { drain() }
        }
    }

    func shutUp() {
        queue.removeAll()
        synth.stopSpeaking(at: .immediate)
        speakingPriority = -1
    }

    func repeatLast() {
        if !lastSpoken.isEmpty { say(lastSpoken, priority: .narrative, cooldown: 0) }
    }

    private func drain() {
        guard let next = queue.first else { speakingPriority = -1; return }
        queue.removeFirst()
        speakingPriority = next.1.rawValue
        lastSpoken = next.0
        let u = AVSpeechUtterance(string: next.0)
        u.voice = AVSpeechSynthesisVoice(identifier: AVSpeechSynthesisVoiceIdentifierAlex)
              ?? AVSpeechSynthesisVoice(language: "en-US")
        u.rate = AVSpeechUtteranceDefaultSpeechRate * 1.05
        synth.speak(u)
    }

    func speechSynthesizer(_ s: AVSpeechSynthesizer, didFinish utterance: AVSpeechUtterance) { drain() }
    func speechSynthesizer(_ s: AVSpeechSynthesizer, didCancel utterance: AVSpeechUtterance) { /* no-op; caller resets */ }
}

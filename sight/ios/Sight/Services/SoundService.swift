import AVFoundation
import SoundAnalysis
import Speech

enum VoiceCommand { case describe, read, stop, repeatLast }

final class SoundService: NSObject, SNResultsObserving, SFSpeechRecognizerDelegate {
    var onCommand: ((VoiceCommand) -> Void)?
    var onClassification: ((String) -> Void)?

    private let engine = AVAudioEngine()
    private var analyzer: SNAudioStreamAnalyzer?
    private let analysisQueue = DispatchQueue(label: "sight.sound.analysis")
    private var recentHits: [(label: String, ts: Date)] = []
    private var lastTranscript: String = ""

    private let recognizer = SFSpeechRecognizer(locale: Locale(identifier: "en-US"))
    private var recognitionTask: SFSpeechRecognitionTask?
    private var recognitionRequest: SFSpeechAudioBufferRecognitionRequest?

    func start() throws {
        SFSpeechRecognizer.requestAuthorization { _ in }
        try AVAudioSession.sharedInstance().setCategory(.playAndRecord, mode: .default, options: [.duckOthers, .mixWithOthers, .allowBluetoothA2DP, .allowAirPlay])

        let input = engine.inputNode
        let format = input.outputFormat(forBus: 0)
        analyzer = SNAudioStreamAnalyzer(format: format)
        let req = try SNClassifySoundRequest(classifierIdentifier: .version1)
        req.windowDuration = CMTimeMakeWithSeconds(0.975, preferredTimescale: 1000)
        req.overlapFactor = 0.5
        try analyzer?.add(req, withObserver: self)

        recognitionRequest = SFSpeechAudioBufferRecognitionRequest()
        recognitionRequest?.shouldReportPartialResults = false
        recognitionRequest?.requiresOnDeviceRecognition = true
        recognitionTask = recognizer?.recognitionTask(with: recognitionRequest!) { [weak self] result, _ in
            guard let self, let result else { return }
            let text = result.bestTranscription.formattedString.lowercased()
            self.lastTranscript = text
            if let cmd = self.match(text) { self.onCommand?(cmd) }
        }

        input.installTap(onBus: 0, bufferSize: 4096, format: format) { [weak self] buffer, when in
            self?.analysisQueue.async { self?.analyzer?.analyze(buffer, atAudioFramePosition: when.sampleTime) }
            self?.recognitionRequest?.append(buffer)
        }
        try engine.start()
    }

    func recentContext(window: TimeInterval = 8) -> (hints: [String], transcript: String) {
        let cutoff = Date().addingTimeInterval(-window)
        let labels = Array(Set(recentHits.filter { $0.ts > cutoff }.map { $0.label })).sorted()
        return (labels, lastTranscript)
    }

    func request(_ request: SNRequest, didProduce result: SNResult) {
        guard let r = result as? SNClassificationResult, let top = r.classifications.first else { return }
        guard top.confidence > 0.5 else { return }
        recentHits.append((top.identifier, Date()))
        if recentHits.count > 24 { recentHits.removeFirst(recentHits.count - 24) }
        onClassification?(top.identifier)
    }

    private func match(_ text: String) -> VoiceCommand? {
        if text.contains("describe") || text.contains("what") && (text.contains("around") || text.contains("ahead") || text.contains("see")) { return .describe }
        if text.contains("read") || text.contains("sign") || text.contains("menu") { return .read }
        if text.contains("stop") || text.contains("quiet") || text.contains("silence") { return .stop }
        if text.contains("repeat") || text.contains("again") { return .repeatLast }
        return nil
    }
}

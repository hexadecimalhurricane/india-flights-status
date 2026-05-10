import Vision
import CoreImage
import AVFoundation

final class VisionService {
    var onUtterance: ((String, SpeechPriority) -> Void)?
    private let queue = DispatchQueue(label: "sight.vision", qos: .userInitiated)
    private var lastFireAt: [String: Date] = [:]
    private var lastTextSpeakAt = Date.distantPast

    private let humanReq: VNDetectHumanRectanglesRequest
    private let textReq: VNRecognizeTextRequest

    init() {
        humanReq = VNDetectHumanRectanglesRequest()
        humanReq.upperBodyOnly = false
        textReq = VNRecognizeTextRequest()
        textReq.recognitionLevel = .fast
        textReq.usesLanguageCorrection = false
    }

    private var inflight = false
    func process(sampleBuffer: CMSampleBuffer) {
        guard !inflight, let pb = CMSampleBufferGetImageBuffer(sampleBuffer) else { return }
        inflight = true
        queue.async { [weak self] in
            defer { self?.inflight = false }
            guard let self else { return }
            let handler = VNImageRequestHandler(cvPixelBuffer: pb, orientation: .right, options: [:])
            try? handler.perform([self.humanReq])
            self.handlePeople(self.humanReq.results ?? [])

            // OCR less often than people detection
            if Date().timeIntervalSince(self.lastTextSpeakAt) > 6 {
                try? handler.perform([self.textReq])
                self.handleText(self.textReq.results ?? [])
            }
        }
    }

    private func handlePeople(_ obs: [VNHumanObservation]) {
        guard let nearest = obs.max(by: { $0.boundingBox.height < $1.boundingBox.height }) else { return }
        let cx = nearest.boundingBox.midX
        let h = nearest.boundingBox.height
        guard h > 0.45 else { return }
        let dir = cx < 0.4 ? "left" : cx > 0.6 ? "right" : "ahead"
        emit("person \(dir)", priority: .salient, key: "person-\(dir)", cooldown: 3.5)
    }

    private func handleText(_ obs: [VNRecognizedTextObservation]) {
        let big = obs.filter { $0.boundingBox.height > 0.06 }
            .sorted { $0.boundingBox.minY > $1.boundingBox.minY }
            .prefix(2)
        guard !big.isEmpty else { return }
        let phrase = big.compactMap { $0.topCandidates(1).first?.string }.joined(separator: ", ")
        guard phrase.count > 2 else { return }
        lastTextSpeakAt = Date()
        emit("sign: \(phrase)", priority: .salient, key: "sign", cooldown: 8)
    }

    private func emit(_ text: String, priority: SpeechPriority, key: String, cooldown: TimeInterval) {
        let now = Date()
        if let last = lastFireAt[key], now.timeIntervalSince(last) < cooldown { return }
        lastFireAt[key] = now
        DispatchQueue.main.async { [weak self] in self?.onUtterance?(text, priority) }
    }
}

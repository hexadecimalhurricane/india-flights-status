import SwiftUI
import AVFoundation

struct ContentView: View {
    @StateObject private var pipeline = Pipeline()

    var body: some View {
        ZStack {
            CameraPreview(session: pipeline.camera.session)
                .ignoresSafeArea()
            VStack {
                Text(pipeline.status)
                    .font(.headline)
                    .padding(10)
                    .background(.black.opacity(0.55))
                    .foregroundStyle(.white)
                    .accessibilityLiveRegion(.polite)
                Spacer()
                Button {
                    pipeline.describeNow(question: "Describe what's ahead of me, hazards first.")
                } label: {
                    Text("Describe")
                        .font(.system(size: 28, weight: .bold))
                        .frame(maxWidth: 360, minHeight: 88)
                        .background(.white)
                        .foregroundStyle(.black)
                        .clipShape(RoundedRectangle(cornerRadius: 22))
                }
                .accessibilityLabel("Describe what's around me")
                .padding(.bottom, 24)
            }
        }
        .task { await pipeline.start() }
    }
}

struct CameraPreview: UIViewRepresentable {
    let session: AVCaptureSession
    func makeUIView(context: Context) -> PreviewView {
        let v = PreviewView()
        v.videoPreviewLayer.session = session
        v.videoPreviewLayer.videoGravity = .resizeAspectFill
        return v
    }
    func updateUIView(_ uiView: PreviewView, context: Context) {}
}

final class PreviewView: UIView {
    override class var layerClass: AnyClass { AVCaptureVideoPreviewLayer.self }
    var videoPreviewLayer: AVCaptureVideoPreviewLayer { layer as! AVCaptureVideoPreviewLayer }
}

@MainActor
final class Pipeline: ObservableObject {
    @Published var status = "Starting…"
    let camera = CameraService()
    let vision = VisionService()
    let sound = SoundService()
    let speech = SpeechService()
    let narrative = NarrativeClient()

    func start() async {
        do {
            try await camera.start { [weak self] sample in
                self?.vision.process(sampleBuffer: sample)
            }
            try sound.start()
            sound.onCommand = { [weak self] cmd in
                guard let self else { return }
                switch cmd {
                case .describe: self.describeNow(question: "Describe what's ahead of me, hazards first.")
                case .read: self.describeNow(question: "Read any signs, prices, or large text in view, top to bottom.")
                case .stop: self.speech.shutUp()
                case .repeatLast: self.speech.repeatLast()
                }
            }
            vision.onUtterance = { [weak self] text, priority in
                self?.speech.say(text, priority: priority)
            }
            speech.say("Sight ready.", priority: .narrative)
            status = "Listening…"
        } catch {
            status = "Failed to start: \(error.localizedDescription)"
        }
    }

    func describeNow(question: String) {
        speech.say("One moment.", priority: .narrative)
        Task {
            guard let frame = camera.latestJPEG(maxWidth: 640) else { return }
            let ctx = sound.recentContext()
            do {
                let text = try await narrative.describe(jpeg: frame, question: question, soundHints: ctx.hints, speech: ctx.transcript)
                speech.say(text, priority: .narrative)
            } catch {
                speech.say("Description failed.", priority: .narrative)
            }
        }
    }
}

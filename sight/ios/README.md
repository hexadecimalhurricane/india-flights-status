# Sight — iOS

SwiftUI app. Build in Xcode 15.4+.

## Setup

1. Create a new Xcode project: **App → SwiftUI → iOS**, name it `Sight`, language Swift.
2. Replace the generated `ContentView.swift` and `SightApp.swift` with the files in `Sight/`.
3. Drag the `Services/` folder into the project.
4. In **Signing & Capabilities** add:
   - **Background Modes → Audio, AirPlay, and Picture in Picture** (so audio keeps playing when the screen is off).
5. In **Info.plist** add the usage strings already present in `Info.plist` here.
6. Set the deployment target to **iOS 17.0**.
7. Set the proxy URL in `Services/NarrativeClient.swift` (or wire to UserDefaults).

## Capabilities used

- `AVCaptureSession` for the rear camera.
- `Vision` (`VNDetectHumanRectanglesRequest`, `VNRecognizeTextRequest`, plus a Core ML object detector — drop in [YOLOv8 CoreML](https://developer.apple.com/machine-learning/models/) for production).
- `SoundAnalysis` (`SNClassifySoundRequest` with Apple's built-in classifier — ~300 environmental categories).
- `Speech` (`SFSpeechRecognizer`) for voice commands.
- `AVSpeechSynthesizer` with a priority queue (see `SpeechService.swift`).
- `AVAudioEnvironmentNode` for spatial earcons over AirPods.

## Files

- `SightApp.swift` — app entry.
- `Views/ContentView.swift` — the only screen.
- `Services/CameraService.swift` — camera + frame delivery.
- `Services/VisionService.swift` — object/text detection, hazard rules.
- `Services/SoundService.swift` — `SNClassifySoundRequest` + speech recognition.
- `Services/SpeechService.swift` — priority TTS queue.
- `Services/EarconService.swift` — spatial earcons via `AVAudioEnvironmentNode`.
- `Services/NarrativeClient.swift` — POST to the proxy.
- `Info.plist` — usage strings + background audio.

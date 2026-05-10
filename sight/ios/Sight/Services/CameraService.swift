import AVFoundation
import UIKit

final class CameraService: NSObject, AVCaptureVideoDataOutputSampleBufferDelegate {
    let session = AVCaptureSession()
    private let queue = DispatchQueue(label: "sight.camera")
    private var onSample: ((CMSampleBuffer) -> Void)?
    private var latestPixelBuffer: CVPixelBuffer?

    func start(_ onSample: @escaping (CMSampleBuffer) -> Void) async throws {
        self.onSample = onSample
        let granted = await AVCaptureDevice.requestAccess(for: .video)
        guard granted else { throw NSError(domain: "Sight", code: 1, userInfo: [NSLocalizedDescriptionKey: "Camera permission denied"]) }

        session.beginConfiguration()
        session.sessionPreset = .hd1280x720

        guard let device = AVCaptureDevice.default(.builtInWideAngleCamera, for: .video, position: .back),
              let input = try? AVCaptureDeviceInput(device: device),
              session.canAddInput(input) else {
            throw NSError(domain: "Sight", code: 2, userInfo: [NSLocalizedDescriptionKey: "No rear camera"])
        }
        session.addInput(input)

        let out = AVCaptureVideoDataOutput()
        out.videoSettings = [kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA]
        out.alwaysDiscardsLateVideoFrames = true
        out.setSampleBufferDelegate(self, queue: queue)
        if session.canAddOutput(out) { session.addOutput(out) }
        out.connection(with: .video)?.videoRotationAngle = 90

        session.commitConfiguration()
        Task.detached(priority: .userInitiated) { [session] in session.startRunning() }
    }

    func captureOutput(_ output: AVCaptureOutput, didOutput sampleBuffer: CMSampleBuffer, from connection: AVCaptureConnection) {
        if let pb = CMSampleBufferGetImageBuffer(sampleBuffer) { latestPixelBuffer = pb }
        onSample?(sampleBuffer)
    }

    func latestJPEG(maxWidth: CGFloat = 640, quality: CGFloat = 0.78) -> Data? {
        guard let pb = latestPixelBuffer else { return nil }
        let ci = CIImage(cvPixelBuffer: pb)
        let scale = min(1, maxWidth / ci.extent.width)
        let scaled = ci.transformed(by: CGAffineTransform(scaleX: scale, y: scale))
        let ctx = CIContext()
        guard let cg = ctx.createCGImage(scaled, from: scaled.extent) else { return nil }
        return UIImage(cgImage: cg, scale: 1, orientation: .right).jpegData(compressionQuality: quality)
    }
}

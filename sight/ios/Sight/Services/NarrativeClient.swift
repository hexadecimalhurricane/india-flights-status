import Foundation

final class NarrativeClient {
    var proxyURL: URL? {
        if let s = UserDefaults.standard.string(forKey: "sight.proxyURL"), let u = URL(string: s) { return u }
        return nil
    }
    var clientToken: String? { UserDefaults.standard.string(forKey: "sight.clientToken") }

    enum NarrativeError: Error { case notConfigured, http(Int), badResponse }

    func describe(jpeg: Data, question: String, soundHints: [String], speech: String) async throws -> String {
        guard let base = proxyURL else { throw NarrativeError.notConfigured }
        var req = URLRequest(url: base.appendingPathComponent("describe"))
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let t = clientToken, !t.isEmpty { req.setValue("Bearer \(t)", forHTTPHeaderField: "Authorization") }
        req.httpBody = try JSONSerialization.data(withJSONObject: [
            "images": [jpeg.base64EncodedString()],
            "question": question,
            "sound_hints": soundHints,
            "speech": speech,
        ])
        req.timeoutInterval = 12
        let (data, resp) = try await URLSession.shared.data(for: req)
        guard let http = resp as? HTTPURLResponse else { throw NarrativeError.badResponse }
        guard (200..<300).contains(http.statusCode) else { throw NarrativeError.http(http.statusCode) }
        let obj = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        return (obj?["text"] as? String) ?? ""
    }
}

import Foundation

enum Base64URL {
    static func encode(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    static func decode(_ s: String) -> Data? {
        var str = s
        let pad = (4 - (str.count % 4)) % 4
        if pad > 0 { str += String(repeating: "=", count: pad) }
        str = str.replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        return Data(base64Encoded: str)
    }
}


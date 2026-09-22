import Dependencies
import Foundation
import XUtils

package struct CommandInput: DependencyKey, Sendable {
    // nil = EOF
    private let readImpl: @Sendable (DataChannel.Limit) async throws -> Data?
    private let closeImpl: @Sendable () async -> Void

    package init(
        read: @escaping @Sendable (DataChannel.Limit) async throws -> Data?,
        close: @escaping @Sendable () async -> Void,
    ) {
        self.readImpl = read
        self.closeImpl = close
    }

    package init(channel: DataChannel) {
        self.init(read: { try await channel.read(upTo: $0) }, close: { channel.finish(error: nil) })
    }

    package func read(upTo limit: DataChannel.Limit = .unlimited) async throws -> Data? {
        try Task.checkCancellation()
        return try await readImpl(limit)
    }

    /// Cancellation discards bytes already consumed for this line.
    package func readLine() async throws -> String? {
        var line = Data()
        while let chunk = try await read(upTo: .newline) {
            line.append(chunk)
            if line.last == UInt8(ascii: "\n") {
                line.removeLast()
                return String(decoding: line, as: UTF8.self).trimmingTrailingCarriageReturn()
            }
        }
        guard !line.isEmpty else { return nil }
        return String(decoding: line, as: UTF8.self).trimmingTrailingCarriageReturn()
    }

    package func close() async {
        await closeImpl()
    }

    package static var liveValue: CommandInput {
        do {
            let input = try DispatchInput(fileDesciptor: .standardInput)
            return Self(read: { try await input.read(upTo: $0) }, close: { await input.close() })
        } catch {
            return Self(read: { _ in throw error }, close: {})
        }
    }

    package static var testValue: CommandInput {
        Self(read: unimplemented("CommandInput.read", placeholder: nil), close: {})
    }
}

private extension String {
    func trimmingTrailingCarriageReturn() -> String {
        hasSuffix("\r") ? String(dropLast()) : self
    }
}

extension DependencyValues {
    package var commandInput: CommandInput {
        get { self[CommandInput.self] }
        set { self[CommandInput.self] = newValue }
    }
}

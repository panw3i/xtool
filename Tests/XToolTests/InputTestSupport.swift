import Foundation
import Testing

/// Releases suspended reads on timeout so a regression cannot hang the test process.
func withInputTestTimeout<T>(
    operation: () async throws -> T,
    cleanup: @escaping @Sendable () async -> Void,
) async throws -> T {
    let watchdog = Task {
        do {
            try await Task.sleep(for: .seconds(5))
        } catch {
            return
        }
        Issue.record("Input operation did not finish within five seconds")
        await cleanup()
    }
    defer { watchdog.cancel() }
    return try await operation()
}

func waitForInputTestCondition(_ condition: () async -> Bool) async throws {
    let deadline = ContinuousClock.now + .seconds(2)
    while !(await condition()) {
        try #require(ContinuousClock.now < deadline, "Timed out waiting for input operation")
        try await Task.sleep(for: .milliseconds(1))
    }
}

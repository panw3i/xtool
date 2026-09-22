import Foundation
import XUtils
import ConcurrencyExtras

actor DispatchInput {
    enum Phase {
        case active
        case closed(Error?)
    }

    static let defaultChunkSize: Int = 64 * 1024

    let queue = DispatchQueue(label: "xtool.dispatch-reader")
    let channel: DispatchIO
    let chunkSize: Int
    private let closeSignal: Task<Void, Never>
    private let dataChannel: DataChannel
    private var hasClosed = false
    private var activeReader: Task<Void, Never>?

    init(fileDesciptor: FileDescriptor, chunkSize: Int = DispatchInput.defaultChunkSize) throws {
        let closeSignal = Task { _ = try? await Task.never() }
        self.closeSignal = closeSignal
        let dataChannel = DataChannel()
        self.dataChannel = dataChannel
        self.chunkSize = chunkSize

        enum InitState {
            case pending
            case done(onClose: @Sendable (Int32) -> Void)
            case failed(Int32)
        }
        let initState = LockIsolated<InitState>(.pending)
        self.channel = DispatchIO(
            type: .stream,
            fileDescriptor: fileDesciptor.rawValue,
            queue: queue,
        ) { err in
            initState.withValue { state in
                switch state {
                case .pending:
                    state = .failed(err)
                case .done(let onClose):
                    onClose(err)
                case .failed: // wat
                    break
                }
            }
        }
        channel.setLimit(lowWater: 1)

        try initState.withValue { state in
            switch state {
            case .pending:
                state = .done { [weak self] err in
                    Task {
                        await self?.didClose(code: err)
                        closeSignal.cancel()
                    }
                }
            case .done: // wat
                break
            case .failed(let err):
                throw Errno(rawValue: err)
            }
        }

        dataChannel.onRequest { [weak self] in
            guard let self else { return }
            Task { await self.onRequest() }
        }
    }

    func read(upTo limit: DataChannel.Limit = .unlimited) async throws -> Data? {
        try await dataChannel.read(upTo: limit)
    }

    private func onRequest() {
        guard activeReader == nil else { return }
        activeReader = Task { [weak self, channel, chunkSize, queue] in
            channel.read(
                offset: 0,
                length: chunkSize,
                queue: queue,
            ) { [weak self] done, data, error in
                guard let self else { return }
                let data = data.map { Data($0) }
                Task {
                    await self.onRead(
                        done: done,
                        data: data,
                        error: error,
                    )
                }
            }
        }
    }

    private func onRead(
        done: Bool,
        data: Data?,
        error: Int32,
    ) {
        if done {
            activeReader = nil
        }
        let data = data.map { Data($0) }
        if let data, !data.isEmpty {
            dataChannel.write(data)
        }
        if error != 0 {
            requestClose(error: Errno(rawValue: error))
        } else if done, data?.isEmpty == true {
            requestClose(error: nil)
        }
    }

    func close() async {
        requestClose(error: nil)
        await closeSignal.value
    }

    private func requestClose(error: Error?) {
        guard !hasClosed else { return }
        hasClosed = true
        channel.close(flags: .stop)
        dataChannel.finish(error: error)
    }

    private func didClose(code: Int32) {
        guard !hasClosed else { return }
        hasClosed = true
        dataChannel.finish(error: code == 0 ? nil : Errno(rawValue: code))
    }

    deinit {
        guard !hasClosed else { return }
        channel.close(flags: .stop)
        dataChannel.finish(error: nil)
    }
}

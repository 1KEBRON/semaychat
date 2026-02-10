import SwiftUI

// MARK: - QR Scanner

struct SemayQRScanSheet: View {
    @Binding var isPresented: Bool
    @State private var lastScanned: String?
    @State private var manualText: String = ""
    @State private var error: String?

    var body: some View {
        NavigationStack {
            VStack(spacing: 14) {
                #if os(iOS)
                CameraScannerView(isActive: true) { code in
                    handle(code: code)
                }
                .frame(height: 320)
                .clipShape(RoundedRectangle(cornerRadius: 12))
                #endif

                VStack(alignment: .leading, spacing: 8) {
                    Text("Or paste a Semay link")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    TextField("semay://…", text: $manualText)
                        .textFieldStyle(.roundedBorder)
                        #if os(iOS)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        #endif
                    Button("Open") {
                        handle(code: manualText)
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(manualText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
                .padding(.horizontal, 16)

                if let error {
                    Text(error)
                        .font(.caption)
                        .foregroundStyle(.orange)
                        .padding(.horizontal, 16)
                }

                Spacer()
            }
            .padding(.top, 12)
            .navigationTitle("Scan")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { isPresented = false }
                }
            }
        }
    }

    private func handle(code: String) {
        let trimmed = code.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        if lastScanned == trimmed { return }
        lastScanned = trimmed

        guard let url = URL(string: trimmed), url.scheme == "semay" else {
            error = "Not a Semay QR/link."
            return
        }

        NotificationCenter.default.post(name: .semayDeepLinkURL, object: url)
        isPresented = false
    }
}

// MARK: - Promise Pay (Create)

struct SemayPromiseCreateSheet: View {
    let business: BusinessProfile

    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.dismiss) private var dismiss

    @State private var satsText: String = ""
    @State private var createdNote: PromiseNote?
    @State private var promiseURLString: String?
    @State private var error: String?

    var body: some View {
        NavigationStack {
            Form {
                Section("Merchant") {
                    Text(business.name)
                        .font(.headline)
                    Text("\(business.category) • \(business.eAddress)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    if !business.plusCode.isEmpty {
                        Text(business.plusCode)
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                }

                if promiseURLString == nil {
                    Section("Amount") {
                        TextField("Amount (sats)", text: $satsText)
                            #if os(iOS)
                            .keyboardType(.numberPad)
                            #endif
                    }

                    Section {
                        Button("Create Promise QR") {
                            create()
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(UInt64(satsText) == nil || (UInt64(satsText) ?? 0) == 0)

                        Text("This creates an offline promise. The merchant scans your QR to accept or reject.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                } else if let promiseURLString {
                    Section("Promise QR") {
                        QRCodeImage(data: promiseURLString, size: 240)
                            .frame(maxWidth: .infinity, alignment: .center)

                        Text(promiseURLString)
                            .font(.system(.caption2, design: .monospaced))
                            .textSelection(.enabled)
                    }

                    if let createdNote {
                        Section("Details") {
                            Text("Amount: \(createdNote.amountMsat / 1000) sats")
                            Text("Expires: \(Date(timeIntervalSince1970: TimeInterval(createdNote.expiresAt)).formatted())")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }

                    Section {
                        ShareLink(item: promiseURLString) {
                            Label("Share", systemImage: "square.and.arrow.up")
                        }
                    }
                }

                if let error {
                    Section {
                        Text(error)
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }
            }
            .navigationTitle("Promise Pay")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button(promiseURLString == nil ? "Cancel" : "Done") { dismiss() }
                }
            }
        }
    }

    private func create() {
        error = nil
        promiseURLString = nil
        createdNote = nil

        guard let sats = UInt64(satsText), sats > 0 else {
            error = "Enter a valid sats amount."
            return
        }
        let msat = sats.multipliedReportingOverflow(by: 1000)
        guard !msat.overflow else {
            error = "Amount too large."
            return
        }

        let note = dataStore.createPromise(merchantID: business.businessID, amountMsat: msat.partialValue)
        guard let envelope = dataStore.makePromiseCreateEnvelope(for: note),
              let data = try? JSONEncoder().encode(envelope)
        else {
            error = "Failed to build promise QR."
            return
        }

        let encoded = Base64URL.encode(data)
        let urlString = "semay://promise/\(encoded)"
        createdNote = note
        promiseURLString = urlString
    }
}

// MARK: - Promise Pay (Inbound)

struct SemayPromiseEnvelopeSheet: View {
    let envelope: SemayEventEnvelope

    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.dismiss) private var dismiss

    @State private var responseURLString: String?
    @State private var error: String?
    @State private var appliedNotice: String?

    var body: some View {
        NavigationStack {
            Form {
                Section("Event") {
                    Text(envelope.eventType.rawValue)
                        .font(.system(.caption, design: .monospaced))
                    Text("From: \(short(envelope.authorPubkey))")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    if let expiry = envelope.expiresAt {
                        Text("Expires: \(Date(timeIntervalSince1970: TimeInterval(expiry)).formatted())")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                switch envelope.eventType {
                case .promiseCreate:
                    promiseCreateSection
                case .promiseAccept, .promiseReject:
                    promiseResponseSection
                default:
                    Section {
                        Text("Unsupported Semay event.")
                            .foregroundStyle(.secondary)
                    }
                }

                if let responseURLString {
                    Section("Response QR") {
                        QRCodeImage(data: responseURLString, size: 240)
                            .frame(maxWidth: .infinity, alignment: .center)
                        Text(responseURLString)
                            .font(.system(.caption2, design: .monospaced))
                            .textSelection(.enabled)
                        ShareLink(item: responseURLString) {
                            Label("Share", systemImage: "square.and.arrow.up")
                        }
                    }
                }

                if let appliedNotice {
                    Section {
                        Text(appliedNotice)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                if let error {
                    Section {
                        Text(error)
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }
            }
            .navigationTitle("Promise")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
            .onAppear {
                if let failure = envelope.validate() {
                    error = "Invalid envelope: \(failure.category.rawValue):\(failure.reason)"
                }
            }
        }
    }

    private var promiseCreateSection: some View {
        let p = envelope.payload
        let promiseID = p["promise_id"] ?? ""
        let merchantID = p["merchant_id"] ?? ""
        let amountMsat = UInt64(p["amount_msat"] ?? "") ?? 0
        let amountSats = amountMsat / 1000
        let expiresAt = Int(p["expires_at"] ?? "") ?? envelope.expiresAt ?? 0
        let business = dataStore.businesses.first(where: { $0.businessID == merchantID })
        let isOwner = business?.ownerPubkey.lowercased() == dataStore.currentUserPubkey()

        return Section("Incoming Promise") {
            if let business {
                Text(business.name)
                    .font(.headline)
                Text("\(business.category) • \(business.eAddress)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("Merchant: \(merchantID)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Text("Amount: \(amountSats) sats")
            if expiresAt > 0 {
                Text("Expires: \(Date(timeIntervalSince1970: TimeInterval(expiresAt)).formatted())")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Text("Promise: \(short(promiseID))")
                .font(.caption2)
                .foregroundStyle(.secondary)

            if !isOwner {
                Text("This device does not control the target business profile, so Accept/Reject is disabled.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            HStack {
                Button("Accept") {
                    acceptReject(status: .accepted)
                }
                .buttonStyle(.borderedProminent)
                .disabled(!isOwner || envelope.validate() != nil)

                Button("Reject") {
                    acceptReject(status: .rejected)
                }
                .buttonStyle(.bordered)
                .disabled(!isOwner || envelope.validate() != nil)
            }
        }
    }

    private var promiseResponseSection: some View {
        let status: PromiseStatus = (envelope.eventType == .promiseAccept) ? .accepted : .rejected
        let promiseID = envelope.payload["promise_id"] ?? ""
        return Section("Response") {
            Text("Status: \(status.rawValue.capitalized)")
                .font(.headline)
            Text("Promise: \(short(promiseID))")
                .font(.caption)
                .foregroundStyle(.secondary)

            Button("Apply To My Ledger") {
                error = nil
                appliedNotice = nil
                if let updated = dataStore.applyPromiseResponseEnvelope(envelope) {
                    appliedNotice = "Updated promise to \(updated.status.rawValue)."
                } else {
                    error = "Promise not found on this device."
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(envelope.validate() != nil)
        }
    }

    private func acceptReject(status: PromiseStatus) {
        error = nil
        appliedNotice = nil
        responseURLString = nil

        guard envelope.validate() == nil else {
            error = "Invalid promise envelope."
            return
        }

        guard let note = dataStore.importPromiseCreateEnvelope(envelope) else {
            error = "Failed to import promise."
            return
        }

        _ = dataStore.updatePromiseStatus(note.promiseID, status: status)

        guard let responseEnvelope = dataStore.makePromiseResponseEnvelope(
            promiseID: note.promiseID,
            merchantID: note.merchantID,
            status: status
        ), let data = try? JSONEncoder().encode(responseEnvelope) else {
            error = "Failed to build response QR."
            return
        }

        responseURLString = "semay://promise-response/\(Base64URL.encode(data))"
        appliedNotice = "Show this QR to the payer so they can update their promise."
    }

    private func short(_ s: String) -> String {
        let cleaned = s.trimmingCharacters(in: .whitespacesAndNewlines)
        if cleaned.count <= 14 { return cleaned }
        return "\(cleaned.prefix(6))…\(cleaned.suffix(6))"
    }
}

// MARK: - Settlement (Manual Proof Entry)

struct SemaySettlementSheet: View {
    let promise: PromiseNote

    @EnvironmentObject private var dataStore: SemayDataStore
    @Environment(\.dismiss) private var dismiss

    @State private var proofType: SettlementProofType = .lightningPaymentHash
    @State private var proofValue: String = ""
    @State private var submittedBy: SettlementSubmitter = .merchant
    @State private var error: String?

    var body: some View {
        NavigationStack {
            Form {
                Section("Promise") {
                    Text("Amount: \(promise.amountMsat / 1000) sats")
                    Text("Status: \(promise.status.rawValue.capitalized)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text("Promise: \(short(promise.promiseID))")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }

                Section("Proof") {
                    Picker("Proof Type", selection: $proofType) {
                        Text("Lightning payment hash").tag(SettlementProofType.lightningPaymentHash)
                        Text("Lightning preimage").tag(SettlementProofType.lightningPreimage)
                    }

                    TextField("Paste proof value", text: $proofValue)
                        #if os(iOS)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        #endif

                    Picker("Submitted By", selection: $submittedBy) {
                        Text("Payer").tag(SettlementSubmitter.payer)
                        Text("Merchant").tag(SettlementSubmitter.merchant)
                    }
                }

                Section {
                    Button("Record Settlement") {
                        record()
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(proofValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)

                    Text("Semay does not hold funds. Pay in your Lightning wallet, then paste the payment hash or preimage here as a receipt.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                if let error {
                    Section {
                        Text(error)
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }
            }
            .navigationTitle("Settlement")
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
            .onAppear {
                let me = dataStore.currentUserPubkey()
                if me == promise.payerPubkey.lowercased() {
                    submittedBy = .payer
                } else {
                    submittedBy = .merchant
                }
            }
        }
    }

    private func record() {
        error = nil
        let trimmed = proofValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        let receipt = dataStore.submitSettlement(
            promiseID: promise.promiseID,
            proofType: proofType,
            proofValue: trimmed,
            submittedBy: submittedBy
        )
        if receipt == nil {
            error = "Failed to record settlement."
        } else {
            dismiss()
        }
    }

    private func short(_ s: String) -> String {
        let cleaned = s.trimmingCharacters(in: .whitespacesAndNewlines)
        if cleaned.count <= 14 { return cleaned }
        return "\(cleaned.prefix(6))…\(cleaned.suffix(6))"
    }
}

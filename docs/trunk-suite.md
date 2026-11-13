# trunk-suite port status

How each N9KOP/sdrtrunk feature maps onto this Go codebase.

| SDRTrunk fork feature | Status in trunk-suite |
|---|---|
| P25: skip encrypted grants | Already in GopherTrunk: `trunking.systems[].encrypted_calls.mode: ignore` |
| P25: skip unaliased talkgroups | Already in GopherTrunk: `scanner.scan_mode: list` |
| P25: ignore data calls | Ported: `trunking.systems[].ignore_data_calls: true` |
| Thinline Radio stream | Ported: `broadcast.thinline` (RdioScanner upload API) |
| Thinline Radio Core stream (hold, call.started / call.ready, upload on request, retry on a new connection, call.uploadFailed, direct upload when offline) | Ported: `broadcast.thinline_core` |
| Thinline Radio health reports and remote control (tuners, channels, playlist, preferences, spectrum) | Not yet ported: bound to SDRTrunk's playlist and tuner model; needs mapping onto GopherTrunk's config and REST API |
| Channel signal level in dB | Partly in GopherTrunk: per-call `SignalDbFS` on call end and in the API |
| Tuner planning (band-edge margin, control-channel alternates on one tuner, traffic frequency log) | Not yet ported |
| RTL R820T/R828D narrow IF filter | Not yet ported |
| Decode quality log, simulated noise, Phase 2 diagnostics | Not yet ported |
| RTL-2832 stream keep-alive, Windows USB hotplug | Not applicable: Java/libusb workarounds |

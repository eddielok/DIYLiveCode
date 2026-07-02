# DIY Live Code Analyzer

Distributed screen capture and analysis system. Sender captures screen in real-time, sends to receiver(s) for analysis using OpenClaw CLI.

## Features

- ✅ Real-time screen capture (1s interval)
- ✅ Interactive receiver IP/hostname input
- ✅ OpenClaw CLI integration for analysis
- ✅ Input/output deduplication (skip duplicates)
- ✅ gRPC for efficient communication
- ✅ Multiple predefined missions (coding_challenge, ui_testing, content_analysis)

## Setup

### 1. Install Dependencies

```bash
cd DIYLiveCode
pip install -r requirements.txt
```

### 2. Generate gRPC Files

```bash
bash generate_protos.sh
```

This creates:

- `proto/capture_pb2.py`
- `proto/capture_pb2_grpc.py`

### 3. Install OpenClaw (Required on Receiver)

```bash
# Install OpenClaw CLI
pip install openclaw
```

Or follow OpenClaw documentation: https://docs.openclaw.ai/

## Quick Start

### Terminal 1: Start Receiver

```bash
python3 receiver.py
# Or specify custom port:
# python3 receiver.py 50052
```

Output:

```
[INFO] Receiver listening on port 50051...
```

### Terminal 2: Start Sender

```bash
python3 sender.py
```

Follow prompts:

```
Enter receiver IP/hostname: 192.168.1.100
Select mission: coding_challenge
```

The sender will:

1. Capture screen every 10 second
2. Hash each capture
3. Skip duplicates automatically
4. Send new captures to receiver
5. Display analysis results from OpenClaw

## Configuration

Edit `config.yaml` to customize:

- `interval`: Capture interval in seconds
- `cache_size`: Deduplication window size
- `missions`: Predefined analysis missions

## Architecture

```
SENDER                          RECEIVER
┌──────────────────────┐      ┌──────────────────────┐
│ Capture Screen       │      │ Listen (gRPC)        │
│ Hash Input           │      │ Save Image           │
│ Check Dedup          │      │ $ openclaw analyze   │
│ Send to [IP:port] ──────────>│ Parse Output         │
└──────────────────────┘      │ Hash Output          │
                              │ Check Dedup          │
                              │ Display Results      │
                              └──────────────────────┘
```

## Deduplication

**Sender side**: Skips sending same screenshot if seen recently (by input hash)
**Receiver side**: Skips displaying same analysis result if seen recently (by output hash)

Both use rolling window cache (last 100 hashes by default).

## Mission Types

| Mission            | Description                                  |
| ------------------ | -------------------------------------------- |
| `coding_challenge` | Analyze code on screen                       |
| `ui_testing`       | Validate UI rendering                        |
| `content_analysis` | Summarize visible content                    |
| `code_debugging`   | Debug code errors and exceptions on screen   |
| `interview_qa`     | Answer interview questions visible on screen |

## Troubleshooting

### "OpenClaw CLI not found"

```bash
pip install openclaw
```

### "Connection refused"

- Ensure receiver is running: `python3 receiver.py`
- Check firewall allows port 50051
- Verify IP address is correct

### "No duplicates being skipped"

- Deduplication cache max is 100 hashes
- If screen changes constantly, fewer will be skipped

## Security Notes

- Currently uses self-signed certificates for testing
- For production, use proper TLS certificates
- Implement authentication for multi-user scenarios
- Consider image compression for large images

## Performance

| Metric           | Expected    |
| ---------------- | ----------- |
| Capture latency  | 100-200ms   |
| Network latency  | 500-1000ms  |
| Analysis latency | 1-5 seconds |
| **Total**        | 2-6 seconds |

## Development

### Run tests

```bash
# TODO: Add tests
```

### Update proto definitions

Edit `proto/capture.proto`, then:

```bash
bash generate_protos.sh
```

## License

MIT

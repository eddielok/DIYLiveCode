# DIY Live Code Analyzer

Distributed screen capture and analysis system. Sender captures screen in real-time, sends to receiver for analysis using **Tesseract OCR + DeepSeek-V3 API**.

## Features

- ✅ Real-time screen capture (1s interval)
- ✅ Interactive receiver IP/hostname input
- ✅ **Tesseract OCR** — local text extraction (~200–500 ms, no network)
- ✅ **DeepSeek-V3 API** — fast text analysis (~1–4 s, direct HTTPS)
- ✅ **Total latency: ~2–5 s** vs OpenClaw CLI's 30–200 s
- ✅ Input/output deduplication (skip duplicates)
- ✅ gRPC for efficient communication
- ✅ Multiple predefined missions (coding_challenge, ui_testing, content_analysis)
- ✅ Live web UI with WebSocket push at `http://localhost:8080`

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

- `capture_pb2.py`
- `capture_pb2_grpc.py`

### 3. Install Tesseract OCR Engine

```bash
# macOS
brew install tesseract

# Ubuntu/Debian
sudo apt install tesseract-ocr
```

### 4. Set DeepSeek API Key

```bash
export DEEPSEEK_API_KEY=sk-your-key-here
```

Or set it directly in `receiver.py` (already configured with a fallback key).

## Quick Start

### Terminal 1: Start Receiver

```bash
python3 receiver.py
# Or specify custom ports:
# python3 receiver.py 50052 8081
```

Output:

```
[INFO] Receiver listening on gRPC port 50051...
[INFO] UI available at http://localhost:8080
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

1. Capture screen every 1 second
2. Hash each capture
3. Skip duplicates automatically
4. Send new captures to receiver
5. Results appear in the web UI at `http://localhost:8080`

## Configuration

Edit `config.yaml` to customize:

- `interval`: Capture interval in seconds
- `cache_size`: Deduplication window size
- `missions`: Predefined analysis missions

## Architecture

```
SENDER                          RECEIVER
┌──────────────────────┐      ┌───────────────────────────────┐
│ Capture Screen       │      │ Listen (gRPC)                 │
│ Hash Input           │      │ Tesseract OCR  (~200–500 ms)  │
│ Check Dedup          │      │ POST text → DeepSeek API      │
│ Send to [IP:port] ──────────>│   (~1–4 s, direct HTTPS)     │
└──────────────────────┘      │ Hash Output                   │
                              │ Check Dedup                   │
                              │ Push to Web UI (WebSocket)    │
                              └───────────────────────────────┘
```

## Performance

| Metric                 | OpenClaw CLI | OCR + DeepSeek |
| ---------------------- | ------------ | -------------- |
| Process spawn overhead | ~500 ms      | None           |
| OCR / image processing | Remote       | ~200–500 ms    |
| AI inference           | 30–200 s     | ~1–4 s         |
| Network hops           | 2            | 1              |
| **Total latency**      | **30–200 s** | **~2–5 s** ✅  |
| Capture latency        | 100–200 ms   | 100–200 ms     |
| gRPC transfer          | 500–1000 ms  | 500–1000 ms    |

## Deduplication

**Sender side**: Skips sending same screenshot if seen recently (by input hash)
**Receiver side**: Skips displaying same analysis result if seen recently (by output hash)

Both use rolling window cache (last 100 hashes by default).

## Mission Types

| Mission            | Description                                  |
| ------------------ | -------------------------------------------- |
| `coding_challenge` | Solve code challenge visible on screen       |
| `ui_testing`       | Validate UI rendering                        |
| `content_analysis` | Summarize visible content                    |
| `code_debugging`   | Debug code errors and exceptions on screen   |
| `interview_qa`     | Answer interview questions visible on screen |

## Troubleshooting

### "tesseract is not installed or it's not in your PATH"

```bash
# macOS
brew install tesseract

# Ubuntu/Debian
sudo apt install tesseract-ocr
```

### "Error: OCR extracted no text from the screenshot"

- The screen may show only images/diagrams with no text
- Try a different mission or ensure text is visible on screen

### "DeepSeek API returned HTTP 401"

- Check your API key: `export DEEPSEEK_API_KEY=sk-your-key`
- Or update the fallback key in `receiver.py`

### "Connection refused"

- Ensure receiver is running: `python3 receiver.py`
- Check firewall allows port 50051
- Verify IP address is correct

## Security Notes

- Keep your DeepSeek API key out of version control — use env vars in production
- Currently uses insecure gRPC for local/LAN use
- For production, add TLS to the gRPC channel

## Development

### Update proto definitions

Edit `proto/capture.proto`, then:

```bash
bash generate_protos.sh
```

## License

MIT

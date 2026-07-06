#!/usr/bin/env python3
"""
Receiver: Listens for screen captures and analyzes them using OpenClaw CLI.
Handles deduplication and outputs results.
"""

import os
os.environ["GRPC_VERBOSITY"] = "NONE"  # Suppress gRPC C-core internal logs

import asyncio
import hashlib
import io
import json
import logging
import subprocess
import tempfile
import time
from collections import deque
from pathlib import Path

import grpc
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn
from PIL import Image
from typing import Optional

import capture_pb2
import capture_pb2_grpc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── FastAPI UI ────────────────────────────────────────────────────────────────

_ui_app = FastAPI()
_ws_clients: set[WebSocket] = set()
_ui_loop: Optional[asyncio.AbstractEventLoop] = None  # uvicorn's event loop

async def _broadcast(data: dict) -> None:
    global _ws_clients
    if not _ws_clients:
        return
    msg = json.dumps(data)
    dead = set()
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    _ws_clients -= dead

def broadcast_from_grpc(payload: dict) -> None:
    """Thread-safe broadcast from gRPC thread into uvicorn's event loop."""
    if _ui_loop is not None and _ui_loop.is_running():
        asyncio.run_coroutine_threadsafe(_broadcast(payload), _ui_loop)

_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Live Code Analyzer</title>
<script>
/* marked.js minimal inline fallback — avoids CDN dependency */
window.marked = window.marked || { parse: function(t){ return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\n/g,'<br>'); } };
</script>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js" onerror="console.warn('CDN unavailable, using fallback renderer')"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0d1117; color: #e6edf3; min-height: 100vh;
  }
  header {
    position: sticky; top: 0; z-index: 10;
    background: #161b22; border-bottom: 1px solid #30363d;
    padding: 14px 24px; display: flex; align-items: center; gap: 16px;
  }
  header h1 { font-size: 16px; font-weight: 600; color: #f0f6fc; }
  #status {
    margin-left: auto; font-size: 13px; display: flex; align-items: center; gap: 6px;
  }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: #3fb950; }
  .dot.offline { background: #f85149; }
  #counter { color: #8b949e; font-size: 13px; }
  #feed { padding: 24px; display: flex; flex-direction: column; gap: 16px; max-width: 900px; margin: 0 auto; }
  .empty { text-align: center; color: #484f58; margin-top: 80px; }
  .empty svg { display: block; margin: 0 auto 16px; opacity: .3; }
  .card {
    background: #161b22; border: 1px solid #30363d; border-radius: 10px;
    animation: slideIn .25s ease;
  }
  @keyframes slideIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
  .card-header {
    padding: 12px 16px; display: flex; align-items: center; gap: 10px;
    border-bottom: 1px solid #21262d; background: #1c2128;
    border-radius: 10px 10px 0 0;
  }
  .badge {
    font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 20px;
    text-transform: uppercase; letter-spacing: .04em;
  }
  .badge-coding_challenge { background: #1f4e31; color: #3fb950; }
  .badge-code_debugging   { background: #4d2020; color: #f85149; }
  .badge-ui_testing       { background: #1a3a5e; color: #58a6ff; }
  .badge-content_analysis { background: #3b2d0e; color: #d29922; }
  .badge-interview_qa     { background: #2d1f4e; color: #bc8cff; }
  .badge-online_test      { background: #1e3a3a; color: #39d0d8; }
  .card-meta { color: #8b949e; font-size: 12px; margin-left: auto; }
  .card-body {
    padding: 16px;
  }
  .card-body h1,.card-body h2,.card-body h3 { color: #f0f6fc; margin: 14px 0 6px; font-size: 15px; }
  .card-body p  { color: #c9d1d9; line-height: 1.65; margin-bottom: 10px; font-size: 14px; }
  .card-body pre {
    background: #0d1117; border: 1px solid #30363d; border-radius: 6px;
    padding: 12px; overflow-x: auto; margin: 10px 0;
    white-space: pre-wrap; word-break: break-word;
  }
  .card-body code { font-family: 'SF Mono', Consolas, monospace; font-size: 13px; color: #e6edf3; white-space: pre-wrap; word-break: break-word; }
  .card-body ul, .card-body ol { padding-left: 20px; color: #c9d1d9; font-size: 14px; line-height: 1.7; }
  .card-body strong { color: #f0f6fc; }
  .card-body li { margin-bottom: 4px; }
  .timing { font-size: 11px; color: #484f58; padding: 6px 16px 10px; }
</style>
</head>
<body>
<header>
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" stroke-width="2">
    <rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>
  </svg>
  <h1>Live Code Analyzer</h1>
  <span id="counter">0 analyses</span>
  <div id="status"><span class="dot offline" id="dot"></span><span id="status-text">Connecting…</span></div>
</header>
<div id="feed">
  <div class="empty" id="empty">
    <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <path d="M9 19c-5 1.5-5-2.5-7-3m14 6v-3.87a3.37 3.37 0 0 0-.94-2.61c3.14-.35 6.44-1.54 6.44-7A5.44 5.44 0 0 0 20 4.77 5.07 5.07 0 0 0 19.91 1S18.73.65 16 2.48a13.38 13.38 0 0 0-7 0C6.27.65 5.09 1 5.09 1A5.07 5.07 0 0 0 5 4.77a5.44 5.44 0 0 0-1.5 3.78c0 5.42 3.3 6.61 6.44 7A3.37 3.37 0 0 0 9 18.13V22"/>
    </svg>
    Waiting for the first capture…
  </div>
</div>
<script>
  marked.setOptions({ breaks: true });

  // Close any unclosed code fences so marked.parse() renders the full content
  function fixCodeFences(text) {
    const fences = (text.match(/^```/gm) || []).length;
    if (fences % 2 !== 0) text = text + '\\n```';
    return text;
  }

  let count = 0;
  const feed = document.getElementById('feed');
  const empty = document.getElementById('empty');
  const dot = document.getElementById('dot');
  const statusText = document.getElementById('status-text');
  const counter = document.getElementById('counter');

  function connect() {
    const ws = new WebSocket(`ws://${location.host}/ws`);
    ws.onopen = () => {
      dot.classList.remove('offline');
      statusText.textContent = 'Live';
    };
    ws.onclose = () => {
      dot.classList.add('offline');
      statusText.textContent = 'Reconnecting…';
      setTimeout(connect, 2000);
    };
    ws.onmessage = ({ data }) => {
      const d = JSON.parse(data);
      if (d.is_duplicate) return;
      empty.style.display = 'none';
      count++;
      counter.textContent = count + ' analys' + (count === 1 ? 'is' : 'es');

      const ts = new Date(d.timestamp * 1000).toLocaleTimeString();
      const badgeClass = 'badge-' + d.mission;
      const card = document.createElement('div');
      card.className = 'card';
      card.innerHTML = `
        <div class="card-header">
          <span class="badge ${badgeClass}">${d.mission.replace(/_/g,' ')}</span>
          <span style="color:#8b949e;font-size:12px">hash: ${d.input_hash}</span>
          <span class="card-meta">${ts}</span>
        </div>
        <div class="card-body">${marked.parse(fixCodeFences(d.output))}</div>
        <div class="timing">⏱ ${d.processing_time_ms}ms</div>`;
      feed.insertBefore(card, feed.firstChild);
    };
  }
  connect();
</script>
</body>
</html>"""

@_ui_app.get("/", response_class=HTMLResponse)
async def ui_index():
    return _UI_HTML

@_ui_app.websocket("/ws")
async def ui_websocket(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        pass
    finally:
        _ws_clients.discard(websocket)

# ─────────────────────────────────────────────────────────────────────────────

def _load_context_file(filename: str) -> str:
    """Load a markdown context file from the same directory as this script."""
    path = Path(__file__).parent / filename
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


class ScreenAnalyzerServicer(capture_pb2_grpc.ScreenAnalyzerServicer):
    """gRPC service for screen analysis."""
    
    def __init__(self):
        self.output_hashes = deque(maxlen=100)  # Recent output hashes
        self.analysis_count = 0
        self.duplicate_output_count = 0
        self._busy = False  # True while analysis is in progress

        # Load CV and JD context once at startup
        cv_content = _load_context_file("CV.md")
        jd_content = _load_context_file("JD.md")
        context_block = ""
        if cv_content:
            context_block += f"\n\n--- CANDIDATE CV ---\n{cv_content}"
        if jd_content:
            context_block += f"\n\n--- JOB DESCRIPTION ---\n{jd_content}"

        self._interview_qa_prompt = (
            "You are helping a candidate answer interview questions in real time.\n"
            "Use the candidate's CV and the job description below as context to give "
            "tailored, specific answers that align the candidate's experience with the role.\n"
            + context_block +
            "\n\n---\n"
            "An interview question is visible on screen. "
            "Output ONLY the direct answer, drawing on the candidate's background above where relevant. "
            "No preamble, no explanation, no restating the question."
        )
        if context_block:
            logger.info("Loaded CV and JD context for interview_qa mission.")
        else:
            logger.warning("No CV.md or JD.md found — interview_qa will run without context.")
    
    def _save_image(self, image_data: bytes) -> str:
        """Save image data to temp file and return path.""" 
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            f.write(image_data)
            return f.name
    
    MISSION_PROMPTS = {
        "coding_challenge": (
            "A coding challenge is visible on screen. "
            "Write the shortest complete working solution. "
            "Use arrays instead of HashMaps where possible. "
            "Output ONLY the code inside one code block. No explanation."
        ),
        "ui_testing": (
            "A UI is visible on screen. "
            "Output ONLY a bullet list of issues found with the exact fix for each (CSS or code). "
            "No explanation or analysis."
        ),
        "content_analysis": (
            "Summarize the visible content in 3-5 bullet points. "
            "Output ONLY the bullet points, nothing else."
        ),
        "code_debugging": (
            "Code with errors is visible on screen. "
            "Output ONLY the corrected code with the bugs fixed. "
            "Add a one-line comment on each changed line explaining the fix. No other text."
        ),
        "online_test": (
            "This is an online technical test covering Coding & Debugging, Agentic AI & LLM Patterns, "
            "System Design & Architecture, and general technical questions.\n"
            "A question or problem is visible on screen. Analyse it carefully and provide a thorough answer.\n"
            "- For coding/debugging questions: output the complete corrected or working code in a code block, "
            "with brief inline comments explaining key decisions.\n"
            "- For Agentic AI & LLM questions: explain the concept clearly, describe relevant patterns "
            "(e.g. ReAct, tool-use, memory, orchestration), and give a concrete example.\n"
            "- For System Design questions: outline the architecture with components, data flow, "
            "scalability considerations, and trade-offs.\n"
            "- For general technical questions: give a precise, well-structured answer with examples.\n"
            "Output a clear, well-structured answer. No preamble, no restating the question."
        ),
        "interview_qa": (
            "You are helping a candidate answer interview questions. "
            "Here is the candidate's CV:\n\n"
            "Name: Eddie Lok, Senior Software Engineer, 15+ years experience in fintech, embedded finance, insurance.\n"
            "Skills: Python, TypeScript, .NET Core, Node.js, Vue.js/React.js, Kotlin, PostgreSQL, MongoDB, AWS/Azure, Docker/Kubernetes, ArgoCD/Jenkins, Kafka, Databricks/Spark, Playwright/Selenium, n8n, AI Agents.\n"
            "Experience:\n"
            "- MMOB Ltd (Aug 2021–Present): Led no-code B2B marketplace tools, onboarded 26+ companies onto embedded finance platform, ISO 27001/FCA compliance, secured ~€6M seed funding, built AI agent with Mastercard MCP server, GraphQL API Hub, AWS CI/CD, Google Cloud Partner.\n"
            "- FWD Life (Jul 2018–Sep 2021): AML/CTF rule-based verification across 8 platforms, auto-underwriting 70%+ insurance products, 60% increase in policy processing, chatbot with 4.5/5 satisfaction and 97% handle rate, cloud Integrated Financial Planning Platform.\n"
            "- BestServe/SunLife (May 2016–Jul 2018): Centralised enterprise data bus, 40% reduction in data interaction lead time, AML/CTF integration, ServiceNow platform.\n"
            "- HKICL (Sep 2014–Mar 2016): E-Cheque mobile app, 60% reduction in cheque clearing time, $23B HKD daily transactions.\n"
            "Education: BSc (Hons) Computer Engineering, City University of Hong Kong.\n\n"
            "Here is the job description being interviewed for:\n\n"
            "Role: Lead Platform Engineer at LightWork AI (London/Remote).\n"
            "Company: Building AI system of action for UK lettings/estate agencies. Voice agent 'Felicity' automates prospecting, viewings, maintenance, compliance, arrears.\n"
            "Responsibilities: Architect/build scalable backend systems and APIs, design microservice/event-driven architectures, lead small engineering team, set standard for agentic AI development, contribute to technical strategy, maintain secure data pipelines.\n"
            "Requirements: 5-8+ years experience, deep Node.js/NestJS expertise, RESTful APIs and microservices, agentic AI/LLM experience, startup background, lead engineer experience.\n"
            "Stack: NestJS, Next.js, Monorepo→Microservices, Google Cloud + Kubernetes, PostgreSQL, MongoDB, Qdrant.\n\n"
            "An interview question is visible on screen. "
            "Answer it as Eddie Lok being interviewed for the Lead Platform Engineer role at LightWork AI. "
            "Output ONLY the direct answer. No preamble, no explanation, no restating the question. "
            "Be specific, confident, and reference relevant experience from the CV where appropriate."
        ),
    }

    def _analyze_with_openclaw(self, image_path: str, mission: str) -> str:
        """Call OpenClaw CLI and return output."""
        try:
            # Use the context-enriched prompt for interview_qa, static prompts for everything else
            if mission == "interview_qa":
                prompt = self._interview_qa_prompt
            else:
                prompt = self.MISSION_PROMPTS.get(mission, f"Analyze the screen for: {mission}")
            cmd = [
                "openclaw", "infer", "model", "run",
                "--file", image_path,
                "--prompt", prompt,
                "--model", "xiaomi-token-plan/mimo-v2.5",
                "--json",
            ]

            # NODE_NO_WARNINGS suppresses the TLS warning openclaw emits to stderr
            env = os.environ.copy()
            env["NODE_NO_WARNINGS"] = "1"

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=210,  # must exceed --timeout-ms (200s) + buffer
                env=env,
            )

            # Strip known Node.js TLS warning from stderr (openclaw emits this harmlessly)
            stderr_clean = "\n".join(
                line for line in (result.stderr or "").splitlines()
                if "NODE_TLS_REJECT_UNAUTHORIZED" not in line
                and "Use node --trace-warnings" not in line
            ).strip()

            if result.returncode != 0:
                error_msg = stderr_clean or f"OpenClaw failed with code {result.returncode}"
                logger.error(f"OpenClaw error: {error_msg}")
                return f"Error: {error_msg}"

            # If stdout is empty but we only got the TLS warning, try stderr as fallback
            stdout = result.stdout.strip()
            if not stdout and not stderr_clean:
                return "Error: OpenClaw returned no output"

            data = json.loads(stdout)
            outputs = data.get("outputs", [])
            if outputs and outputs[0].get("text"):
                return outputs[0]["text"].strip()
            return f"Error: No description returned by OpenClaw"
        
        except subprocess.TimeoutExpired:
            return "Error: OpenClaw analysis timed out (>110s)"
        except FileNotFoundError:
            return "Error: OpenClaw CLI not found. Install with: pip install openclaw"
        except Exception as e:
            return f"Error: {str(e)}"
    
    def _hash_data(self, data: str) -> str:
        """Generate SHA-256 hash of output."""
        return hashlib.sha256(data.encode()).hexdigest()
    
    async def AnalyzeScreen(self, request, context):
        """Process screen capture and analyze with OpenClaw."""
        if self._busy:
            logger.debug("Receiver busy — telling sender to retry")
            return capture_pb2.AnalysisResult(
                output="",
                is_duplicate=True,
                output_hash="",
                processing_time_ms=0
            )

        self._busy = True
        start_time = time.time()
        input_hash = request.input_hash[:8]  # Short hash for logging
        mission = request.mission
        
        try:
            # Save image
            image_path = self._save_image(request.image_data)
            self.analysis_count += 1
            logger.info(
                f"Received capture #{self.analysis_count}\n"
                f"  Input hash: {input_hash}...\n"
                f"  Mission: {mission}\n"
                f"  Image size: {len(request.image_data)} bytes"
            )
            
            # Analyze with OpenClaw
            analysis_output = self._analyze_with_openclaw(image_path, mission)
            output_hash = self._hash_data(analysis_output)
            
            # Check for duplicate output
            is_duplicate = output_hash in self.output_hashes
            if is_duplicate:
                self.duplicate_output_count += 1
                logger.info("Output is duplicate, skipping display")
            else:
                self.output_hashes.append(output_hash)
                logger.info(
                    f"Analysis complete\n"
                    f"Output:\n{analysis_output}\n"
                )

            # Broadcast to UI via thread-safe call into uvicorn's event loop.
            payload = {
                "mission": mission,
                "input_hash": input_hash,
                "output": analysis_output,
                "is_duplicate": is_duplicate,
                "output_hash": output_hash,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "timestamp": time.time(),
            }
            broadcast_from_grpc(payload)
            
            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            # Clean up
            Path(image_path).unlink(missing_ok=True)
            
            # Return result
            self._busy = False
            return capture_pb2.AnalysisResult(
                output=analysis_output,
                is_duplicate=is_duplicate,
                output_hash=output_hash,
                processing_time_ms=processing_time_ms
            )

        except Exception as e:
            logger.error(f"Error processing capture: {e}")
            self._busy = False
            return capture_pb2.AnalysisResult(
                output=f"Error: {str(e)}",
                is_duplicate=False,
                output_hash="",
                processing_time_ms=int((time.time() - start_time) * 1000)
            )


def _run_uvicorn(ui_port: int) -> None:
    """Run uvicorn in its own thread with its own event loop."""
    global _ui_loop
    # Create a new event loop for this thread and store it so gRPC can
    # schedule broadcasts into it via run_coroutine_threadsafe.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    config = uvicorn.Config(
        _ui_app,
        host="0.0.0.0",
        port=ui_port,
        log_level="info",
        ws="websockets",
        loop="none",  # use the loop we created above
    )
    server = uvicorn.Server(config)

    # Store the loop AFTER uvicorn is configured so broadcasts go to the right loop
    _ui_loop = loop
    try:
        loop.run_until_complete(server.serve())
    finally:
        _ui_loop = None


async def serve(port: int = 50051, ui_port: int = 8080):
    """Start gRPC server and web UI."""
    import threading

    # Run uvicorn in a separate thread so it gets its own event loop,
    # avoiding interference with gRPC's asyncio machinery.
    ui_thread = threading.Thread(target=_run_uvicorn, args=(ui_port,), daemon=True)
    ui_thread.start()

    server = grpc.aio.server()
    capture_pb2_grpc.add_ScreenAnalyzerServicer_to_server(
        ScreenAnalyzerServicer(),
        server
    )

    server.add_insecure_port(f"0.0.0.0:{port}")

    logger.info(f"Receiver listening on gRPC port {port}...")
    logger.info(f"UI available at http://localhost:{ui_port}")

    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 50051
    ui_port = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
    asyncio.run(serve(port, ui_port))

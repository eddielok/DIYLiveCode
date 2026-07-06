#!/usr/bin/env python3
"""
Sender: Captures screen and sends to receiver(s) for analysis.
Auto-captures every 15 seconds. Press Control+SPACE to capture immediately
and reset the 15s timer. Includes deduplication and interactive IP/name configuration.
"""

import asyncio
import hashlib
import io
import logging
import threading
import time
from collections import deque

import grpc
import mss
import typer
from PIL import Image
from pynput import keyboard

import capture_pb2
import capture_pb2_grpc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer()

AUTO_CAPTURE_INTERVAL = 15  # Seconds between automatic captures
HOTKEY_DEBOUNCE_SECONDS = 2  # Minimum seconds between manual hotkey presses

# Global state
class SenderState:
    def __init__(self):
        self.input_hashes = deque(maxlen=100)  # Recent hashes
        self.capture_count = 0
        self.skipped_count = 0
        self.trigger = asyncio.Event()   # Set when Control+SPACE is pressed or timer fires
        self.loop: asyncio.AbstractEventLoop | None = None
        self._last_hotkey_time: float = 0.0  # Timestamp of last accepted hotkey press
        self._ctrl_pressed: bool = False     # Track whether Control key is held
        self._reset_timer: bool = False      # Flag to reset the auto-capture timer


def capture_screenshot() -> bytes:
    """Capture screen and return as JPEG bytes."""
    with mss.mss() as sct:
        monitor = sct.monitors[1]  # Primary monitor
        screenshot = sct.grab(monitor)

        # Convert to PIL Image
        img = Image.frombytes('RGB', screenshot.size, screenshot.rgb)

        # Resize if needed
        if img.width > 1280:
            ratio = 1280 / img.width
            new_height = int(img.height * ratio)
            img = img.resize((1280, new_height), Image.Resampling.LANCZOS)

        # Save to bytes
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=90)
        return buffer.getvalue()


def hash_data(data: bytes) -> str:
    """Generate SHA-256 hash of data."""
    return hashlib.sha256(data).hexdigest()


async def send_to_receiver(
    target: str,
    image_data: bytes,
    mission: str,
    input_hash: str,
    state: SenderState
) -> bool:
    """Send screenshot to receiver via gRPC. Returns True if analysis ran."""

    # Check if input is duplicate
    if input_hash in state.input_hashes:
        state.skipped_count += 1
        logger.info(f"⏭  Duplicate screenshot — skipped (hash: {input_hash[:8]}...)")
        return False

    state.input_hashes.append(input_hash)

    try:
        # Parse target (can be IP:port or hostname)
        if ':' in target:
            host, port_str = target.rsplit(':', 1)
            port = int(port_str)
        else:
            host = target
            port = 50051

        # Connect to receiver
        async with grpc.aio.insecure_channel(f"{host}:{port}") as channel:
            stub = capture_pb2_grpc.ScreenAnalyzerStub(channel)

            # Create request
            request = capture_pb2.ScreenCapture(
                image_data=image_data,
                mission=mission,
                timestamp=int(time.time() * 1000),
                input_hash=input_hash
            )

            # Send — timeout must exceed the receiver's AI processing time (up to 210s)
            result = await stub.AnalyzeScreen(request, timeout=220)

            if not result.is_duplicate:
                logger.info(
                    f"✓ Analysis result (Mission: {mission}, "
                    f"Target: {host}:{port})"
                )
                logger.info(f"Output:\n{result.output}")
                return True
            else:
                logger.debug("Receiver busy, will retry")
                return False

    except Exception as e:
        logger.error(f"Failed to send to {target}: {e}")
        return False


def start_hotkey_listener(state: SenderState) -> None:
    """
    Listen for Control+SPACE globally in a background thread.
    When pressed, triggers an immediate capture and resets the auto-capture timer.
    """
    def on_press(key: keyboard.Key) -> None:
        # Track Control key state
        if key in (keyboard.Key.ctrl, keyboard.Key.ctrl_l, keyboard.Key.ctrl_r):
            state._ctrl_pressed = True
            return

        if key == keyboard.Key.space and state._ctrl_pressed and state.loop is not None:
            now = time.time()
            elapsed = now - state._last_hotkey_time
            if elapsed < HOTKEY_DEBOUNCE_SECONDS:
                remaining = HOTKEY_DEBOUNCE_SECONDS - elapsed
                logger.debug(f"⏳ Control+SPACE debounced — {remaining:.1f}s remaining")
                return
            state._last_hotkey_time = now
            state._reset_timer = True  # Signal the capture loop to reset its timer
            # Thread-safe: schedule the event set on the asyncio loop
            state.loop.call_soon_threadsafe(state.trigger.set)

    def on_release(key: keyboard.Key) -> None:
        # Clear Control key state on release
        if key in (keyboard.Key.ctrl, keyboard.Key.ctrl_l, keyboard.Key.ctrl_r):
            state._ctrl_pressed = False

    listener = keyboard.Listener(on_press=on_press, on_release=on_release)
    listener.daemon = True
    listener.start()
    logger.info("⌨️  Global hotkey listener started — press Control+SPACE anywhere to capture immediately")


async def capture_loop(
    target: str,
    mission: str,
) -> None:
    """Auto-capture every 15s, or immediately when Control+SPACE is pressed (resets timer)."""
    state = SenderState()
    state.loop = asyncio.get_running_loop()

    # Start global keyboard listener in background thread
    start_hotkey_listener(state)

    logger.info(
        f"Ready.\n"
        f"  Target : {target}\n"
        f"  Mission: {mission}\n"
        f"  Auto-capturing every {AUTO_CAPTURE_INTERVAL}s. Press Control+SPACE to capture immediately and reset timer. Ctrl+C to quit."
    )

    try:
        while True:
            # Wait for either the auto-capture interval or a manual hotkey trigger
            try:
                await asyncio.wait_for(
                    asyncio.shield(state.trigger.wait()),
                    timeout=AUTO_CAPTURE_INTERVAL
                )
                # Hotkey was pressed
                state.trigger.clear()
                if state._reset_timer:
                    state._reset_timer = False
                    logger.info("📸 Control+SPACE pressed — capturing screen (timer reset)...")
                else:
                    logger.info("📸 Control+SPACE pressed — capturing screen...")
            except asyncio.TimeoutError:
                # Auto-capture interval elapsed
                logger.info(f"⏱  Auto-capture triggered (every {AUTO_CAPTURE_INTERVAL}s)...")

            image_data = capture_screenshot()
            input_hash = hash_data(image_data)
            state.capture_count += 1

            await send_to_receiver(target, image_data, mission, input_hash, state)

    except KeyboardInterrupt:
        logger.info(
            f"\nStopped.\n"
            f"  Total captures : {state.capture_count}\n"
            f"  Duplicates skipped: {state.skipped_count}\n"
            f"  Unique sent    : {state.capture_count - state.skipped_count}"
        )


@app.command()
def start(
    target: str = typer.Option(
        "127.0.0.1",
        prompt="Enter receiver IP/hostname (e.g., 192.168.1.100 or 192.168.1.100:50051)",
        help="IP address or hostname of receiver machine"
    ),
    mission: str = typer.Option(
        "coding_challenge",
        prompt="Select mission (1=coding_challenge, 2=ui_testing, 3=content_analysis, 4=code_debugging, 5=interview_qa, 6=online_test)",
        help="Type of analysis to perform"
    ),
) -> None:
    """Start sender — auto-captures every 15s, or press Control+SPACE to capture immediately."""
    mission_map = {
        "1": "coding_challenge",
        "2": "ui_testing",
        "3": "content_analysis",
        "4": "code_debugging",
        "5": "interview_qa",
        "6": "online_test",
    }
    mission = mission_map.get(mission.strip(), mission)
    asyncio.run(capture_loop(target, mission))


if __name__ == "__main__":
    app()

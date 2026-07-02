#!/usr/bin/env python3
"""
Sender: Captures screen and sends to receiver(s) for analysis.
Press SPACE (globally, even when unfocused) to trigger a capture.
Includes deduplication and interactive IP/name configuration.
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

# Global state
class SenderState:
    def __init__(self):
        self.input_hashes = deque(maxlen=100)  # Recent hashes
        self.capture_count = 0
        self.skipped_count = 0
        self.trigger = asyncio.Event()   # Set when SPACE is pressed
        self.loop: asyncio.AbstractEventLoop | None = None


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
    Listen for SPACE key globally in a background thread.
    When pressed, signals the async capture loop via state.trigger.
    """
    def on_press(key: keyboard.Key) -> None:
        if key == keyboard.Key.space and state.loop is not None:
            # Thread-safe: schedule the event set on the asyncio loop
            state.loop.call_soon_threadsafe(state.trigger.set)

    listener = keyboard.Listener(on_press=on_press)
    listener.daemon = True
    listener.start()
    logger.info("⌨️  Global hotkey listener started — press SPACE anywhere to capture")


async def capture_loop(
    target: str,
    mission: str,
) -> None:
    """Wait for SPACE press, then capture and send."""
    state = SenderState()
    state.loop = asyncio.get_running_loop()

    # Start global keyboard listener in background thread
    start_hotkey_listener(state)

    logger.info(
        f"Ready.\n"
        f"  Target : {target}\n"
        f"  Mission: {mission}\n"
        f"  Press SPACE (anywhere) to capture and analyse. Ctrl+C to quit."
    )

    try:
        while True:
            # Wait until SPACE is pressed
            await state.trigger.wait()
            state.trigger.clear()

            logger.info("📸 SPACE pressed — capturing screen...")
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
        prompt="Select mission (1=coding_challenge, 2=ui_testing, 3=content_analysis, 4=code_debugging, 5=interview_qa)",
        help="Type of analysis to perform"
    ),
) -> None:
    """Start sender — press SPACE globally to trigger a screen capture."""
    mission_map = {
        "1": "coding_challenge",
        "2": "ui_testing",
        "3": "content_analysis",
        "4": "code_debugging",
        "5": "interview_qa",
    }
    mission = mission_map.get(mission.strip(), mission)
    asyncio.run(capture_loop(target, mission))


if __name__ == "__main__":
    app()

# main.py
import threading
import time
from src.pipeline_runner import PipelineRunner
from config.settings import Config
from src.ingestion.listener import start_listener

if __name__ == "__main__":
    # Initialize pipeline runner 
    runner = PipelineRunner(Config)

    # Start listener in a background thread
    listener_thread = threading.Thread(
        target=start_listener,
        args=(runner.config,), 
        daemon=True
    )
    listener_thread.start()

    # give listener a moment to bind before sending data
    time.sleep(1)
    runner.run()

    print("Pipeline finished. Listener will exit as program ends.")

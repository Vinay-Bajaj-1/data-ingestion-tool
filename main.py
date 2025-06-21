from src.pipeline_runner import PipelineRunner
from config.settings import Config
if __name__ == "__main__":
    runner = PipelineRunner(Config)
    runner.run()

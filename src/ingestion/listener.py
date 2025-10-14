# src/ingestion/listener.py
import zmq
import pickle
import zlib
from src.ingestion.clickhouse import ClickhouseConnect

def start_listener(config, host="0.0.0.0", port=5555): 
    """
    Listener service that receives DataFrames and inserts into ClickHouse.
    Uses its own ClickHouse client to avoid concurrent session issues.
    """
    # Create a separate ClickHouse client for this thread
    clickhouse_client = ClickhouseConnect(
        host=config.CLICKHOUSE_HOST,
        username=config.CLICKHOUSE_USERNAME,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DATABASE,
        table_name=config.CLICKHOUSE_TABLE
    )

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind(f"tcp://{host}:{port}")
    print(f"📡 Listener started on {host}:{port}")

    while True:
        compressed = socket.recv()
        message = pickle.loads(zlib.decompress(compressed))

        df = message["df"]
        ticker = message["ticker"]
        table_name = message["table_name"]

        print(f"✅ Received DataFrame with shape: {df.shape} for ticker '{ticker}', table '{table_name}'")

        try:
            clickhouse_client.push_data_to_database(table_name, df, ticker)
            print(f"Inserted {len(df)} rows for ticker '{ticker}' into table '{table_name}'")
        except Exception as e:
            print(f"Failed to insert DataFrame for ticker '{ticker}': {e}")

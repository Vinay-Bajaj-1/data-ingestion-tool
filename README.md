# 📊 Data Ingestion Tool

An end-to-end pipeline to ingest **1-minute OHLCV equity data** into a **ClickHouse** database.  
This tool supports:

- Reading data from local CSV files, or  
- Pulling end-of-day (EOD) data from the AngelOne API  

It automatically handles data preprocessing, identifies missing data, and ensures efficient insertion into the database.

## 🧐 Features

- ✅ Ingest 1-minute OHLCV equity data  
- ✅ Supports local directory or AngelOne API as data source  
- ✅ Identifies last available timestamp in database and fetches only new required data  
- ✅ Fetches and identifies newly listed equity scripts from AngelOne API  
- ✅ Cleans and preprocesses data before ingestion  
- ✅ Efficient and scalable insertion into ClickHouse supporting chunked uploads for large datasets  

## 🧰 How It Works

### 🗂️ Mode 1: Local CSV Ingestion

```bash
1. Reads OHLCV `.csv` files from a specified local folder.
2. Cleans and validates the data (timestamp formatting, sorting, type casting).
3. Inserts the preprocessed records into ClickHouse.
```

### 🌐 Mode 2: EOD API-Based Ingestion (AngelOne)

```bash
1. Fetches the latest list of tradeable equity instruments via the AngelOne API.
2. For each instrument:
   - Checks if it already exists in the ClickHouse database.
   - If it exists, fetches the latest available timestamp.
   - Downloads new 1-minute OHLCV EOD equity data from that point onward.
3. Cleans and preprocesses the data.
4. Inserts the new data into ClickHouse.
```

> ⚠️ This tool does **not support live intraday ingestion**. Data should be pulled **after market close (EOD only).**

## ⚙️ How to Use

All configuration is handled through a `.env` file. There are no command-line arguments required.

To run the tool:

```bash
python main.py
```

The pipeline automatically reads settings from your `.env` file to determine the mode (`api` or `local`) and other configurations.

## 🔐 .env Configuration

Create a `.env` file in your project root with the following keys:

```env
# --- Data Source ---
DATA_SOURCE_MODE=api
LOCAL_DATA_FOLDER=./data/csv/

# --- AngelOne API Credentials ---
ANGELONE_API=semityapi
ANGELONE_SECRET_KEY=your_secret_key
ANGEL_ONE_PASSWORD=your_password
ANGEL_ONE_TOKEN=your_token
ANGEL_ONE_USER_ID=your_user_id
ANGEL_ONE_PIN=1100
ANGEL_ONE_API_SCRIPT=your_script_name

# --- ClickHouse Configuration ---
CLICKHOUSE_HOST=localhost
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=stock_data
CLICKHOUSE_TABLE=stock_ohlcv

# ---- ZeroMQ ----
LISTENER_IP=127.0.0.1
LISTENER_PORT=5555
```


## 🧪 ClickHouse Table Schema

```sql
CREATE TABLE stock_ohlcv (
  ticker LowCardinality(String),
  timestamp DateTime,
  open Float32,
  high Float32,
  low Float32,
  close Float32,
  volume Int64
) ENGINE = MergeTree()
ORDER BY (ticker, timestamp);
```

## 📁 Project Structure

```
src/
├── downloader/
│   ├── angelone_api_client.py
│   └── fetch_local_data.py
├── preprocess/
│   └── preprocess.py
├── ingestion/
│   ├── clickhouse.py
│   ├── ingest_single.py
│   └── listener.py     
├── utils/
│   └── logger.py
main.py

```

## ✅ Requirements

- Python 3.8+
- ClickHouse server running
- AngelOne SmartAPI account

Install dependencies:

```bash
pip install -r requirements.txt
```

## 🍰 Contribution Guidelines

Feel free to fork the repo, submit issues or pull requests.  
Suggestions to improve ingestion speed, support new APIs, or refactor logic are welcome.

## 💻 Built With

- Python  
- AngelOne SmartAPI  
- ClickHouse

# ⚡ BTC Institutional-Grade Data Terminal

A real-time, high-frequency data pipeline that streams, processes, and visualizes Bitcoin market data. This project replicates a professional fintech stack using a Docker-based architecture to handle live WebSocket feeds and time-series storage.

## 🏗 System Architecture

The project is built as a coordinated data pipeline managed by **Docker Compose**:

1.  **Ingestion Layer (`producer.py`):** Connects to the Coinbase Pro WebSocket API to stream raw L2 trade data.
2.  **Processing Layer (`consumer.py`):** The "Brain" of the operation. It consumes raw messages from Redpanda (Kafka), identifies "Whale" trades (>0.5 BTC), calculates volatility spikes, and handles database persistence.
3.  **Storage Layer (TimescaleDB):** A PostgreSQL-based time-series database optimized for fast writes and complex financial queries (like OHLCV resampling).
4.  **Visualization Layer (`dashboard.py`):** A Bloomberg-style Streamlit terminal featuring live Candlestick charts (10s, 1m, 5m intervals) and a real-time "Tape" of market activity.

## 🛠 Tech Stack

* **Language:** Python 3.11+
* **Streaming:** Redpanda (Kafka-compatible event streaming)
* **Database:** TimescaleDB (Time-series PostgreSQL)
* **Visualization:** Streamlit & Plotly
* **Containerization:** Docker & Docker Compose
* **Management:** pgAdmin4

## 🚀 Getting Started

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* Git installed.

### Installation & Execution
1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YOUR_USERNAME/btc-terminal.git](https://github.com/YOUR_USERNAME/btc-terminal.git)
   cd btc-terminal
   Launch the entire stack:
   docker-compose up --build
   Access the Interfaces:

📊 Live Terminal: http://localhost:8501

🐘 Database Manager (pgAdmin): http://localhost:8080

Login: admin@admin.com / admin

📈 Key Features
Dynamic Candlesticks: Real-time resampling of trade data into 10-second, 1-minute, and 5-minute candles.

Whale Detection: Instant filtering of high-volume institutional orders.

Volatility Monitoring: Automated alerts for rapid price swings within specified time windows.

Data Persistence: Uses Docker Volumes (./postgres_data) to ensure market history survives container restarts.

# Databricks Streaming PoC

This repository demonstrates a Proof of Concept (PoC) for real-time data processing using PySpark Structured Streaming. It provides different processing architectures (monolithic, modular, and hybrid) to showcase various approaches to streaming data pipelines.

## Features
- **Monolithic, Modular, and Hybrid Streaming Processors**: Compare different architectural styles for streaming data processing.
- **Socket-based Streaming Source**: Reads data from a socket for easy local testing.
- **Event Filtering and Aggregation**: Filters events by type and aggregates counts over time windows.
- **Query Plan Export**: Saves the physical query plan for analysis.
- **Console Output**: Streams results to the console for real-time monitoring.

## File Structure
- `monolithic_processor.py`: Monolithic streaming pipeline implementation.
- `modular_processor.py`: Modular streaming pipeline implementation.
- `hybrid_processor.py`: Hybrid streaming pipeline implementation.
- `config.py`: Contains the schema and configuration used by the processors.
- `run_poc.py`: Entry point to run the PoC.
- `visualization.py`: Tools for visualizing performance and results.

## Requirements
- Python 3.8+
- PySpark

## Installation
1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd databricks-poc
   ```
2. Install dependencies:
   ```bash
   pip install pyspark
   ```

## Usage
1. **Start a socket server** (for example, using `nc`):
   ```bash
   nc -lk 9999
   ```
   Send JSON lines matching the schema defined in `config.py`.

2. **Run the PoC**:
   ```bash
   python run_poc.py
   ```
   This will start the streaming pipeline and output results to the console.

## Customization
- Modify `config.py` to change the schema or event types.
- Adjust the processors to experiment with different streaming logic.

## License
This project is for demonstration purposes only.

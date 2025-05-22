# Bitunix to TradeZella Sync

This project fetches futures trades from the Bitunix API and exports them to a CSV file compatible with TradeZella's Generic Template.

## Features
- Fetches historical futures trades from Bitunix.
- Generates request signatures as per Bitunix API documentation.
- Exports trades to TradeZella's Generic CSV format.
- Persists the last exported timestamp to avoid duplicate exports.

## Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd BitunixTradezellaSync
   ```

2. Create a `credentials.json` file in the project directory with the following structure:
   ```json
   {
     "api_key": "YOUR_API_KEY",
     "secret_key": "YOUR_SECRET_KEY",
     "start_time": "2025-05-21T00:00:00Z"
   }
   ```

3. Install dependencies (if any):
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the script to fetch trades and export them to a CSV file:
```bash
python bitunix_to_tradezella.py --output new_trades.csv
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

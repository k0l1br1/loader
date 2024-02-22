# loader
A tool to load and store candlestick data 

```
usage: loader -s <symbol> [options]
    -s, --symbol        The pair for which need to load the prices data
    -n, --is-new        The flag to init new instance for a symbol
    -t, --start-time    Date (UTC) from which to start downloading
                        (format like 2024-02-19 03:37:05)
    --show-start        Show the close date (UTC) of the first candle
    --show-end          Show the close date (UTC) of the last candle
```
run like `loader -s btcusdt -n -t '2024-02-22 00:00:00'`


#la solucion va a ser pasar la vela como parametros de la funcion de los tasks

#viejos scripts para hacer merge originales sin cambios
import ccxt.pro as ccxtpro
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize exchange
exchange = ccxtpro.binanceusdm({'enableRateLimit': True})
symbol = 'BTC/USDT'
timeframe = '1m'  # 1-minute candlesticks

async def watch_candles():
    last_timestamp = None  # Stores the previous candle's timestamp
    last_candle = None  # Stores the last completed candle

    while True:
        try:
            # Fetch the latest candle
            candles = await exchange.watch_ohlcv(symbol, timeframe, since=None, limit=1, params={})
            current_candle = candles[-1]  # Get the latest candle
            current_timestamp = current_candle[0]  # Timestamp of the new candle (open time)

            if last_timestamp is not None and current_timestamp != last_timestamp:
                # A new candle has started → meaning the previous one is now completed
                formatted_time = exchange.iso8601(last_timestamp)  # Convert timestamp to readable format
                logging.info(f"Completed Candle ({formatted_time}): {last_candle}")

            # Update tracking variables
            last_timestamp = current_timestamp
            last_candle = current_candle

        except Exception as e:
            logging.error(f"WebSocket error: {e}")
            logging.info("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

async def main():
    try:
        await watch_candles()
    finally:
        logging.info("Closing exchange connection...")
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())



if __name__ == "__main__":
    while True:
        now = datetime.now()    
        # Run 4-hour task
        #if now.hour % 4 == 0 and now.minute == 0:
        if now.hour % 4 == 0 and now.minute == 0:  
            print(f"4-hour at {datetime.now()}")
            
        # Run 30-minute task
        if now.minute % 30 == 0:  # This will also trigger at hour mark, so no separate check for 4-hour mark necessary for 30-minute tasks
            print(f"Executing 30-minutes task at {datetime.now()}")
            
        # Run 5-minute task
        if now.minute % 5 == 0:
            print(f"Executing 5-minutes task at {datetime.now()}")
            
        # Run 1-minute task
        print(f"Executing 1-minute update at {datetime.now()}")
               
        calculate_sleep_minute()
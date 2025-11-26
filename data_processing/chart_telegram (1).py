
#TELEGRAM FUNCTIONS

#################SEND TELEGRAM IMAGE
def send_image_to_telegram_universal(image_path, token):
    """
    Send an image to a specified Telegram chat using a bot token.

    Parameters:
        token (str): The Telegram bot token.
        chat_id (str): The Telegram chat ID where the image will be sent.
        image_path (str): The path to the image file to send.

    Returns:
        response: The response from the Telegram API.
    """
    #token = '7339625753:AAFyWUShuuJBZTnilnDkuZ4YQ9tc1TQpycw'
    chat_id = '5640035546'
    url = f'https://api.telegram.org/bot{token}/sendPhoto'
    files = {'photo': open(image_path, 'rb')}
    data = {'chat_id': chat_id}
    response = requests.post(url, files=files, data=data)
    return response.json()

    # Example usage:
    # token = 'your_telegram_bot_token'
    # chat_id = 'your_chat_id'
    # image_path = 'BTC_USDT_4h_QR_chart.png'
    # response = send_image_to_telegram(token, chat_id, image_path)
    # print(response)
    #SEND TELEGRAM IMAGE 
    #send_image_to_telegram_universal(filename, token)
    
#################SEND TELEGRAM IMAGE

#################SEND TELEGRAM VARIABLE



def telegram_send_variable_with_prefix(variable, token_identifier, prefix):
    """
    Send a variable as a text message to a specified Telegram chat using a bot token
    selected based on a time interval identifier (e.g., '4h', '30m', '5m', '1m').

    Parameters:
        variable: The variable to send (can be any data type).
        token_identifier (str): Identifier to select the appropriate bot token ('4h', '30m', '5m', '1m').
        prefix (str): The prefix to prepend to the message, default is "start_time:".
        chat_id (str): The Telegram chat ID where the message will be sent.

    Returns:
        response: The response from the Telegram API.
    """
    chat_id = '5640035546'  # Assuming chat_id is fixed as before
    # Token selection based on the identifier
    tokens = {
        '4h': '7339625753:AAFyWUShuuJBZTnilnDkuZ4YQ9tc1TQpycw',
        '30m': '7357699862:AAHpwC0SX3UBYYLk4sdORWU2HVMQvNEOLB4',
        '5m': '7155157037:AAGHJcDxji_eWqAgOH8Wo6V7FYystgM1NeQ',
        '1m': '6638879849:AAFYhcY51OFVCUHQqCHyqM2DiVdbiaSHMlg'
    }
    token = tokens.get(token_identifier)  # Get the token based on the identifier
    if not token:
        raise ValueError("Invalid token identifier provided.")

    url = f'https://api.telegram.org/bot{token}/sendMessage'
    message = f"{prefix} {str(variable)}"  # Construct message with prefix and variable
    data = {
        'chat_id': chat_id,
        'text': message
    }
    response = requests.post(url, data=data)
    return response.json()

# Example usage:
# response = send_variable_with_prefix({'key': 'value', 'date': datetime.datetime.now()}, '4h')
# print(response)


#################SEND TELEGRAM VARIABLE


def send_telegram_message(message):
    async def send_telegram_alert(bot_token, chat_id, message):
        """
        Asynchronously sends a message to a specified Telegram chat via a bot.
    
        :param bot_token: Your Telegram bot's token, as a string.
        :param chat_id: The chat ID of the destination chat.
        :param message: The message to send.
        """
        bot = Bot(token=bot_token)
        await bot.send_message(chat_id=chat_id, text=message)
    
    # Example usage
    bot_token = '6759814080:AAG-gFhltbEpktt2w1jhg2wn6W4fVnCKYpQ'
    chat_id = '5640035546'
    #message = 'Hello, this is an alert message2!'
    
    # Running the asynchronous function using asyncio.run for standalone scripts
    asyncio.run(send_telegram_alert(bot_token, chat_id, message))

#PLOT FUNCTION

def plot_chart(df_filtered, filename):
    #################PLOT
    #entra la varialbe output de process_quantile_regression que seria de QuantileRegressionLines.run_all

    # Identify the quantile regression columns that passed the correlation filter
    qr_columns = [col for col in df_filtered.columns if 'QR_' in col] # Get all QR columns

    # Create additional plots for the quantile regression lines
    add_plots = [mpf.make_addplot(df_filtered[qr_column], color='blue') for qr_column in qr_columns]

    # Current timestamp to append to the filename
    #filename = f'BTC_USDT_4h_QR_chart.png'

    # Plot the candlestick chart with the quantile regression lines
    mpf.plot(df_filtered, type='candle', addplot=add_plots, style='charles',
             title='BTC/USDT with Quantile Regression Lines', figratio=(35.0, 8.0), savefig=filename)

    #################PLOT
    


# SAVE TO TXT FUNCTIONS

def save_to_txt2(file_path, data):
    """
    Save the given string data to a .txt file.
    
    Arguments:
    file_path -- The path (including file name) where the .txt file should be saved.
    data -- The string data to save to the file.
    """
    if not isinstance(data, str):
        data = str(data)  # Convert to string if it's not a string

    # Write the data to the file
    with open(file_path, 'w') as file:
        file.write(data)


#save_to_txt('/root/script/script_output/lines_coordinates_4h.txt', str_data)

def save_to_txt(file_path, data, max_retries=5, delay=3):
    """
    Save the given string data to a .txt file with file locking and retry mechanism.
    
    Arguments:
    file_path -- The path (including file name) where the .txt file should be saved.
    data -- The string data to save to the file.
    max_retries -- Maximum number of retries if the file is locked or unavailable.
    delay -- Delay (in seconds) between retries.
    """
    if not isinstance(data, str):
        data = str(data)  # Convert to string if it's not a string
    
    retries = 0
    while retries < max_retries:
        try:
            # Open the file in write mode
            with open(file_path, 'w') as file:
                # Try to acquire an exclusive lock (LOCK_EX)
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Write the data to the file
                file.write(data)
                fcntl.flock(file, fcntl.LOCK_UN)  # Release the lock
                print(f"File saved successfully at {file_path}")
                return  # Exit the function if successful

        except BlockingIOError:
            # If the file is already locked by another process
            print(f"File is locked by another process. Retrying in {delay} seconds...")
        except (OSError, IOError) as e:
            # Handle other file-related errors
            print(f"Error: {e}. Retrying in {delay} seconds...")
        
        retries += 1
        time.sleep(delay)  # Wait before retrying

    print(f"Failed to save file after {max_retries} retries.")
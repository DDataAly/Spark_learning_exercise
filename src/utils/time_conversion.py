from datetime import datetime, timedelta

def convert_datetime_to_unix_in_ms (datetime_time):
    '''
    Converts Python datetime object to Unix time in milliseconds

    Args:
        datetime_time (datetime): Python datetime object 
    Return:
        int: Unix time in milliseconds from 1 Jan 1970 

    Example: 2025-11-11 10:08:01.521105 -> 1762605502839
    '''
    
    return int(datetime_time.timestamp() *1000)
    
def convert_unix_in_ms_to_datetime(unix_time_in_ms):
    # 1762605502839 -> 2025-11-11 10:08:01.521105
    return datetime.fromtimestamp(unix_time_in_ms/1000)


def calculate_chunk_end_time (chunk_start_time, chunk_size_in_months):
    chunk_end_time = convert_unix_in_ms_to_datetime (chunk_start_time) + timedelta(days = 30*chunk_size_in_months) #2025-11-11 10:08:01.521105
    return convert_datetime_to_unix_in_ms (chunk_end_time) #1762605502839



def time_elapsed(elapsed_time):

    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = elapsed_time % 60

    if hours > 0:
        friendly_time = f"{hours} hours, {minutes} minutes, and {seconds:.2f} seconds"
    else:
        friendly_time = f"{minutes} minutes and {seconds:.2f} seconds"

    return friendly_time


def format_bytes(size):
    # 2**10 = 1024
    power = 1024
    n = 0
    power_labels = {0 : 'bytes', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size > power:
        size /= power
        n += 1
    return f"{round(size, 2)} {power_labels[n]}"


def interp(x, x1, y1, x2, y2):
    """Perform linear interpolation.

    It's easier to use numpy.interp but to avoid adding the
    dependency we are adding this simple function

    Args:
        x (float): The x-value to interpolate.
        x1 (float): The x-value of the first point.
        y1 (float): The y-value of the first point.
        x2 (float): The x-value of the second point.
        y2 (float): The y-value of the second point.

    Returns:
        float: The interpolated y-value.
    """
    return y1 + (x - x1) * (y2 - y1) / (x2 - x1)

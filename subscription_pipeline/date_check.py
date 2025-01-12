from datetime import datetime

def validate_month_format(month: str):
    """
    Validates that the given string matches the format '%Y-%m' (e.g., '2024-01').
    Raises a ValueError if the format is invalid.
    
    :param month: The month string to validate.
    :return: True if the format is valid, otherwise raises a ValueError.
    """
    try:
        # Attempt to parse the string using the specified format
        datetime.strptime(month, '%Y-%m')
        return True
    except ValueError:
        # Raise an exception if parsing fails
        raise ValueError(f"Invalid month format: {month}. Expected format is 'YYYY-MM'.")

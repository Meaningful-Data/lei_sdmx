"""
Utility functions for SDMX data validation using FMR (Fusion Metadata Registry).
"""

import json
from time import sleep, time
from typing import Any, Dict, List, Optional

from requests import get, post

# ------------------------------
# ------------ FMR -------------
# ------------------------------

STATUS_IN_PROCESS = ['Initialising', 'Analysing', 'Validating',
                     'Consolidating']

# The list of load status which means is an error
STATUS_ERRORS = ['IncorrectDSD', 'InvalidRef', 'MissingDSD',
                 'Error']

STATUS_COMPLETED = ["Complete"]


def __handle_status(response_status):
    if response_status.json()['Status'] == 'Complete':
        if response_status.json()['Datasets'][0]['Errors']:
            return response_status.json()['Datasets'][0]['ValidationReport']
        else:
            return []
    if response_status.json()['Status'] in STATUS_ERRORS:
        raise Exception(response_status.json())


def __validation_status_request(status_url: str,
                                uid: str,
                                max_retries: int = 10,
                                interval_time: float = 0.5):
    """
    Polls the FMR instance to get the validation status of an uploaded file

    :param status_url: The URL for checking the validation status
    :type status_url: str

    :param uid: the unique identifier we have to send to the request
    :type uid: str

    :param max_retries: The maximum number of retries for
                        checking validation status
    :type max_retries: int

    :param interval_time: The interval time between retries in seconds
    :type interval_time: float

    :return: The validation status if successful

    :exception: raise an exception if the validation status
                is not found in the response
    :exception: raise an exception if the current time exceeds the timeout
    """

    # Record the starting time for the entire validation process
    start_global = time()

    # Initialize variables for tracking time intervals
    start = time()
    interval_counter = 0

    # Get the current time
    current = time()

    # Calculate the total timeout based on max retries and interval time
    timeout = max_retries * interval_time

    while current - start_global < timeout:
        current = time()

        # Calculate the time interval from the start of the validation process
        interval_start = current - start_global
        interval = current - start

        # Skip the current iteration if the time interval is less than the
        # specified interval time
        if interval_start <= interval_time:
            continue

        # Perform a get request to the server to check the load status
        response_status = get(url=status_url,
                              params={'uid': uid})

        # Check if the 'Status' key is present in the response JSON
        if 'Status' not in response_status.json():
            raise Exception("Error: Status not found in response")

        # Check if the status is still in process
        if response_status.json()['Status'] in STATUS_IN_PROCESS:
            if interval > interval_time:
                interval_counter += 1
                start = time()

            # Check if the maximum number of retries is reached
            if interval_counter == max_retries:
                raise Exception(
                    f"Error: Max retries exceeded ({interval_counter})")

        # Return the handled status if the validation is still in process
        return __handle_status(response_status)

    # Raise an exception if the total timeout is exceeded
    raise Exception(f"Timeout {timeout} exceeded on status request.")


def get_validation_status(status_url: str,
                          uid: str,
                          max_retries: int = 10,
                          interval_time: float = 0.5
                          ):
    """
    Gets the validation status of file uploaded using the FMR instance.

    :param status_url: The URL for checking the validation status
    :type status_url: str

    :param uid: The unique identifier of the uploaded file
    :type uid: str

    :param max_retries: The maximum number of retries for checking validation
                        status (default is 10)
    :type max_retries: int

    :param interval_time: The interval time between retries
                          in seconds (default is 0.5)
    :type interval_time: float

    :return: The validation status if successful
    :exception: raise an exception if the validation status
                is not found in the response
    :exception: raise an exception if the current time exceeds the timeout
    """

    # Pause execution for the specified interval time
    sleep(interval_time)

    # Perform a GET request to the server to check the load status
    response_status = get(url=status_url,
                          params={'uid': uid})

    # Check if the status is still in process
    if response_status.json()['Status'] in STATUS_IN_PROCESS:
        # If in process, recursively call the function with retries
        return __validation_status_request(status_url=status_url,
                                           uid=uid,
                                           max_retries=max_retries,
                                           interval_time=interval_time)
    # Return the handled status if the validation is complete
    return __handle_status(response_status)


def validate_data_fmr(csv_text: str,
                      host: str = 'localhost',
                      port: int = 8080,
                      use_https: bool = False,
                      delimiter: str = 'comma',
                      max_retries: int = 10,
                      interval_time: float = 0.5
                      ):
    """
    Validates an SDMX CSV file by uploading it to an FMR instance
    and checking its validation status

    :param csv_text: The SDMX CSV text to be validated
    :type csv_text: str

    :param host: The FMR instance host (default is 'localhost')
    :type host: str

    :param port: The FMR instance port (default is 8080 for HTTP and
                 443 for HTTPS)
    :type port: int

    :param use_https: A boolean indicating whether to use HTTPS
                     (default is False)
    :type use_https: bool

    :param delimiter: The delimiter used in the CSV file
                      (options: 'comma', 'semicolon', 'tab', 'space')
    :type delimiter: str

    :param max_retries: The maximum number of retries for checking
                        validation status (default is 10)
    :type max_retries: int

    :param interval_time: The interval time between retries
                          in seconds (default is 0.5)
    :type interval_time: float

    :exception: Exception with error details if validation fails
    """

    if use_https and port == 8080:
        port = 443

    # Constructing the base URL based on the provided parameters
    base_url = f'http{"s" if use_https else ""}://{host}:{port}'

    # Constructing the upload URL for the FMR instance
    upload_url = base_url + '/ws/public/data/load'

    # Checking if the provided delimiter is valid
    if delimiter not in ('comma', 'semicolon', 'tab', 'space'):
        raise ValueError('Delimiter must be comma, semicolon, tab or space')

    # Defining headers for the request
    headers = {'Data-Format': f'csv;delimiter={delimiter}'}

    # Perform a POST request to the server with the CSV data as an attachment
    response = post(upload_url,
                    files={'uploadFile': csv_text.replace("dataprovision", "datastructure")},
                    headers=headers)

    # Check the response from the server
    if not response.status_code == 200:
        raise Exception(response.text, response.status_code)

    # Constructing the URL for checking the validation status
    status_url = base_url + '/ws/public/data/loadStatus'

    # Getting the uid from the request response
    uid = response.json()['uid']

    # Return the validation status by calling a separate function
    return get_validation_status(status_url=status_url,
                                 uid=uid,
                                 max_retries=max_retries,
                                 interval_time=interval_time)

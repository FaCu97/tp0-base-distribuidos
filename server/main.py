#!/usr/bin/env python3

from configparser import ConfigParser
from common.server import Server
import logging
import os
import signal
import sys

def build_graceful_shutdown_handler(server):
    def graceful_shutdown(signum, frame):
        _ = frame
        logging.info('action: shutdown_signal | result: success | signal: %s', signum)
        server.graceful_shutdown()
        sys.exit(0)

    return graceful_shutdown

def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["listen_backlog"] = int(os.getenv('SERVER_LISTEN_BACKLOG', config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]))
        config_params["total_agencies"] = int(os.getenv('SERVER_TOTAL_AGENCIES', config["DEFAULT"]["SERVER_TOTAL_AGENCIES"]))
        config_params["logging_level"] = os.getenv('LOGGING_LEVEL', config["DEFAULT"]["LOGGING_LEVEL"])
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    port = config_params["port"]
    listen_backlog = config_params["listen_backlog"]
    total_agencies = config_params["total_agencies"]

    if total_agencies <= 0:
        raise ValueError("SERVER_TOTAL_AGENCIES must be greater than zero")

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug(
        'action: config | result: success | port: %s | listen_backlog: %s | total_agencies: %s | logging_level: %s',
        port,
        listen_backlog,
        total_agencies,
        logging_level,
    )

    # Initialize server and start server loop
    server = Server(port, listen_backlog, total_agencies)
    signal.signal(signal.SIGTERM, build_graceful_shutdown_handler(server))
    server.run()

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()

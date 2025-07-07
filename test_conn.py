#!/usr/bin python3
import os
import socket
import argparse

def main():
    parser = argparse.ArgumentParser(
        description="Test connection to remote host"
    )
    
    # Add optional arguments with defaults
    parser.add_argument(
        "-a",
        "--host",
        default="95.31.13.220",
        help="Remote host name (default: %(default)s)"
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=8265,
        help="Port (default: %(default)s)"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Print the values
    print(f"Trying to connect to {args.host}:{args.port}")
    try:
        code = socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((args.host, args.port))
        print('Port', 'opened' if not code else f'not opened ({code})')

    except socket.timeout:
        print("Connection timed out.")
    except ConnectionRefusedError:
        print("Connection refused by the target machine.")
    except ConnectionResetError:
        print("Connection reset by peer.")
    except socket.gaierror as e:
        print(f"Address-related error: {e}")
    except OSError as e:
        print(f"Socket error: {e.errno} - {os.strerror(e.errno)}") #type:ignore
    except Exception as ex:
        print(f'Error: {ex}')

if __name__ == "__main__":
    main()


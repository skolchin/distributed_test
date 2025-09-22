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
        default="95.31.13.220:8265",
        help="Remote host name with optional port (default: %(default)s)"
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=0,
        help="Port (default: 8265)"
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=5,
        help="Timeout in seconds (default: %(default)s)"
    )
    
    # Parse arguments
    args = parser.parse_args()
    if not args.port and args.host.partition(':')[2]:
        args.host, _, args.port = args.host.partition(':')
        args.port = int(args.port)
    if not args.port:
        args.port = 8265
    
    # Print the values
    print(f"Trying to connect to {args.host}:{args.port}")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(args.timeout or 5)
        code = sock.connect((args.host, args.port))
        print('Port', 'opened' if not code else f'not opened ({code})')

    except socket.timeout:
        print("Connection timed out")
    except ConnectionRefusedError:
        print("Connection refused by the target machine")
    except ConnectionResetError:
        print("Connection reset by peer")
    except socket.gaierror as e:
        print(f"Address-related error: {e}")
    except OSError as e:
        print(f"Socket error: {e.errno} - {os.strerror(e.errno)}") #type:ignore
    except (KeyboardInterrupt, SystemExit):
        print('')
    except Exception as ex:
        print(f'Error: {ex}')

if __name__ == "__main__":
    main()


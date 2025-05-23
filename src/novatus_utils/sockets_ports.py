"""Collection of any socket/port or tcp/ip related utils"""

import socket


def port_in_use(port: int, host="localhost") -> bool:
    """Check if port is in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0

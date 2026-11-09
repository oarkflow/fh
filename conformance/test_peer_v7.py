"""Start a native Go server and exercise the separate Python encrypted peer."""
import os
import secrets
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path


class PeerInterop(unittest.TestCase):
    def test_independent_python_client_to_go_udp(self):
        binary = os.environ.get('MIRA_V7_ECHO')
        if not binary:
            self.skipTest('MIRA_V7_ECHO not set: Go server binary required')
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        secret = secrets.token_hex(32)
        server = subprocess.Popen([binary, '-listen', f'127.0.0.1:{port}'],
                                  env={**os.environ, 'MIRA_PSK': secret},
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            time.sleep(.2)
            runner = subprocess.run([sys.executable, str(Path(__file__).with_name('independent_peer_v4.py')),
                                     '--server', f'127.0.0.1:{port}', '--keyhex', secret,
                                     '--message', 'python-go-interoperability'],
                                    capture_output=True, text=True, timeout=12)
            self.assertEqual(runner.returncode, 0, runner.stderr)
            self.assertIn('echo: python-go-interoperability', runner.stdout)
        finally:
            server.terminate()
            try: server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=3)

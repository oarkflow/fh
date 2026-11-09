import unittest
from pathlib import Path
import mira_wire_v4 as mira

HERE = Path(__file__).parent

class TestWireV4(unittest.TestCase):
    def test_header_fixture(self):
        expected = bytes.fromhex((HERE / 'hello_header_v4.hex').read_text())
        self.assertEqual(expected, mira.header(1, 0x0102030405060708, 0, 0, 80))
        self.assertEqual(mira.parse_header(expected + b'\x00' * 80)['connection'], 0x0102030405060708)
    def test_ack_fixture(self):
        expected = bytes.fromhex((HERE / 'ack_ranges_frame_v4.hex').read_text())
        self.assertEqual(mira.frames(expected)[0], (15, 0, b'\x01\x02\x05'))
    def test_rejects_noncanonical(self):
        with self.assertRaises(ValueError): mira.frames(b'\x01\x00\x81\x00X')
    def test_v4_extensions(self):
        self.assertEqual(mira.frames(mira.frame(16,b'abc'))[0][2], b'abc')
        self.assertEqual(mira.frames(mira.frame(17,b'\x01\x02'))[0][0], 17)

if __name__ == '__main__': unittest.main()

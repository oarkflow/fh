#!/usr/bin/env python3
"""Deterministic source archive; rejects common secret and generated-file patterns.
This is a reproducible-build aid, not independent artifact attestation.
"""
import argparse
import hashlib
import os
import pathlib
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
SKIP_DIRS = {'.git', '.pytest_cache', '__pycache__', 'node_modules', 'build', '.venv', 'vendor'}
SKIP_SUFFIXES = {'.key', '.p12', '.pfx', '.so', '.a', '.aar', '.xcframework', '.pyc', '.exe', '.pem'}
SKIP_NAMES = {'.env', '.env.production', '.DS_Store', 'id_rsa', 'id_ed25519'}

def package(path: pathlib.Path) -> None:
    files = []
    for base, dirs, names in os.walk(ROOT):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
        for name in sorted(names):
            source = pathlib.Path(base) / name
            rel = source.relative_to(ROOT)
            if source.is_symlink() or source.suffix in SKIP_SUFFIXES or source.name in SKIP_NAMES or any(p in SKIP_DIRS for p in rel.parts):
                continue
            if source.resolve() == path.resolve():
                continue
            if source.stat().st_size > 20 * 1024 * 1024:
                raise ValueError(f'oversize file: {rel}')
            files.append((source, rel))
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as out:
        for source, rel in sorted(files, key=lambda row: str(row[1])):
            info = zipfile.ZipInfo('mira-t-v7/' + rel.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o755 if os.access(source, os.X_OK) else 0o644) << 16
            out.writestr(info, source.read_bytes(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
    print(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output', type=pathlib.Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    package(args.output)

from enum import Enum


class FileHeaders(Enum):
    jpg = ['ffd8ffdb', 'ffd8ffe000104a4649460001', 'ffd8ffee', 'ffd8ffe1', 'ffd8ffe0', 'ffd8ffe8']
    png = ['89504e470d0a1a0a', '89504e47da1aa']


def determine_file(script_hex):
    for file_header in FileHeaders:
        for identifier in file_header.value:
            if identifier in script_hex:
                return file_header.name
    return ''

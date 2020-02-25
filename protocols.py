from enum import Enum


class Protocols(Enum):
    ascribe = ['41534352494245']
    bitproof = ['42495450524f4f46']
    blockaibindedpixsy = ['1f00']
    blocksign = ['4253']
    blockstoreblockstack = ['6964', '5888', '5808']
    chainpoint = []
    coinspark = ['53504b']
    colu = ['4343']
    counterparty = ['434e545250525459']
    counterpartytest = ['5858']
    cryptocopyright = ['43727970746f54657374732d', '43727970746f50726f6f662d', '6a2843727970746f50726f6f662d']
    diploma = []
    eternitywall = ['4557']
    factom = ['466163746f6d2121', '464143544f4d3030', '4661', '4641']
    lapreuve = ['4c61507265757665']
    monegraph = ['4d47']
    omni = ['6f6d6e69']
    openassets = ['4f41']
    openchain = ['4f43']
    originalmy = ['4f5249474d59']
    proofofexistence = ['444f4350524f4f46']
    provebit = ['50726f7665426974']
    remembr = ['524d4264', '524d4265']
    smartbit = ['53422e44']
    stampd = ['5354414d50442323']
    stampery = ['5331', '5332', '5333', '5334', '5335']
    universityofnicosia = ['554e6963444320']
    veriblock = ['50000']


def determine_protocol(script_hex):
    for protocol in Protocols:
        for identifier in protocol.value:
            if script_hex.startswith(identifier):
                return protocol.name
    return ''

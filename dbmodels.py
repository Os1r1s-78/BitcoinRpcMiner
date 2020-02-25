from psycopg2.extensions import AsIs, ISQLQuote


class TransactionOutput:
    def __init__(self, txhash, blocktime, blockhash, outvalue, outtype, outasm, outhex, protocol, fileheader):
        self.txhash = txhash
        self.blocktime = blocktime
        self.blockhash = blockhash
        self.outvalue = outvalue
        self.outtype = outtype
        self.outasm = outasm
        self.outhex = outhex
        self.protocol = protocol
        self.fileheader = fileheader

    def __len__(self):
        return 1

    def __getitem__(self, item):
        return self

    # To make it work with psycopg2's execute_values bulk insert method
    # https://stackoverflow.com/questions/52562562/psycopg2-execute-values-with-list-of-class-objects/52564059
    def __conform__(self, protocol):
        if protocol is ISQLQuote:
            return AsIs('default, \'{0}\', {1}, \'{2}\', {3}, \'{4}\', \'{5}\', \'{6}\', \'{7}\', \'{8}\''.format(
                self.txhash, self.blocktime, self.blockhash,
                self.outvalue, self.outtype, self.outasm,
                self.outhex, self.protocol, self.fileheader))
        return None


class FrequencyAnalysis:
    id = 0
    dataday = None
    nulldata = 0
    p2pk = 0
    p2pkh = 0
    p2ms = 0
    p2sh = 0
    unknowntype = 0


class SizeAnalysis:
    id = 0
    dataday = None
    avgsize = 0
    outputs = 0


class ProtocolAnalysis:
    id = 0
    dataday = None
    ascribe = 0
    bitproof = 0
    blockaibindedpixsy = 0
    blocksign = 0
    blockstoreblockstack = 0
    chainpoint = 0
    coinspark = 0
    colu = 0
    counterparty = 0
    counterpartytest = 0
    cryptocopyright = 0
    diploma = 0
    emptytx = 0
    eternitywall = 0
    factom = 0
    lapreuve = 0
    monegraph = 0
    omni = 0
    openassets = 0
    openchain = 0
    originalmy = 0
    proofofexistence = 0
    provebit = 0
    remembr = 0
    smartbit = 0
    stampd = 0
    stampery = 0
    universityofnicosia = 0
    unknownprotocol = 0
    veriblock = 0

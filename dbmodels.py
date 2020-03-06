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
    def __init__(self):
        self.reset()

    def reset(self):
        self.id = 0
        self.dataday = None
        self.nulldata = 0
        self.p2pk = 0
        self.p2pkh = 0
        self.p2ms = 0
        self.p2sh = 0
        self.unknowntype = 0

    id = 0
    dataday = None
    nulldata = 0
    p2pk = 0
    p2pkh = 0
    p2ms = 0
    p2sh = 0
    unknowntype = 0


class SizeAnalysis:
    def __init__(self):
        self.reset()

    def reset(self):
        self.id = 0
        self.dataday = None
        self.avgsize = 0
        self.outputs = 0

    id = 0
    dataday = None
    avgsize = 0
    outputs = 0


class FileAnalysis:
    def __init__(self):
        self.reset()

    def reset(self):
        self.avi_wav = 0
        self.bin_file = 0
        self.bpg = 0
        self.bz2 = 0
        self.crx = 0
        self.dat = 0
        self.deb = 0
        self.doc_xls_ppt = 0
        self.docx_xlsx_pptx = 0
        self.dmg = 0
        self.exe_dll = 0
        self.flac = 0
        self.flv = 0
        self.gif = 0
        self.gz = 0
        self.ico = 0
        self.iso = 0
        self.jpg = 0
        self.lz = 0
        self.mkv = 0
        self.mp3 = 0
        self.mp4 = 0
        self.ogg = 0
        self.pdf = 0
        self.png = 0
        self.psd = 0
        self.rar = 0
        self.rtf = 0
        self.seven_z = 0
        self.sqlite = 0
        self.tar = 0
        self.threegp = 0
        self.tiff = 0
        self.webp = 0
        self.wmv = 0
        self.xml = 0
        self.zip = 0

    avi_wav = 0
    bin_file = 0
    bpg = 0
    bz2 = 0
    crx = 0
    dat = 0
    deb = 0
    doc_xls_ppt = 0
    docx_xlsx_pptx = 0
    dmg = 0
    exe_dll = 0
    flac = 0
    flv = 0
    gif = 0
    gz = 0
    ico = 0
    iso = 0
    jpg = 0
    lz = 0
    mkv = 0
    mp3 = 0
    mp4 = 0
    ogg = 0
    pdf = 0
    png = 0
    psd = 0
    rar = 0
    rtf = 0
    seven_z = 0
    sqlite = 0
    tar = 0
    threegp = 0
    tiff = 0
    webp = 0
    wmv = 0
    xml = 0
    zip = 0


class ProtocolAnalysis:
    def __init__(self):
        self.reset()

    def reset(self):
        self.id = 0
        self.dataday = None
        self.ascribe = 0
        self.bitproof = 0
        self.blockaibindedpixsy = 0
        self.blocksign = 0
        self.blockstoreblockstack = 0
        self.chainpoint = 0
        self.coinspark = 0
        self.colu = 0
        self.counterparty = 0
        self.counterpartytest = 0
        self.cryptocopyright = 0
        self.diploma = 0
        self.emptytx = 0
        self.eternitywall = 0
        self.factom = 0
        self.lapreuve = 0
        self.monegraph = 0
        self.omni = 0
        self.openassets = 0
        self.openchain = 0
        self.originalmy = 0
        self.proofofexistence = 0
        self.provebit = 0
        self.remembr = 0
        self.smartbit = 0
        self.stampd = 0
        self.stampery = 0
        self.universityofnicosia = 0
        self.unknownprotocol = 0
        self.veriblock = 0

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

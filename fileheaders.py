from enum import Enum


class FileHeaders(Enum):
    avi_wav = ['52494646']
    # bmp = ['424d']
    bin_file = ['424c4932323351']
    bpg = ['425047fb']
    bz2 = ['425a68']
    crx = ['43723234']
    dat = ['504d4f43434d4f43']
    deb = ['213c617263683e']
    doc_xls_ppt = ['d0cf11e0a1b11ae1']
    docx_xlsx_pptx = ['504b0304']
    dmg = ['7801730d626260']
    exe_dll = ['4d5a9000']
    flac = ['664c6143']
    flv = ['464c5601']
    gif = ['474946383761', '474946383961']
    gz = ['1f8b0808']
    ico = ['00000100']
    iso = ['4344303031']
    jpg = ['ffd8ffdb', 'ffd8ffe000104a4649460001', 'ffd8ffee', 'ffd8ffe1', 'ffd8ffe0', 'ffd8ffe8']
    lz = ['4c5a4950']
    mkv = ['1a45dfa393428288']
    mp3 = ['4944332e', '49443303']
    mp4 = ['00000018667479706d703432']
    ogg = ['4f676753']
    pdf = ['255044462d']
    png = ['89504e470d0a1a0a', '89504e47da1aa']
    psd = ['38425053']
    rar = ['526172211a0700', '526172211a070100']
    rtf = ['7b5c72746631']
    seven_z = ['377abcaf271c']
    sqlite = ['53514c69746520666f726d6174203300']
    tar = ['1f8b0800', '1f9d90', '425a68', '7573746172']
    threegp = ['667479703367']
    tiff = ['492049', '49492a00', '4d4d002a', '4d4d002b']
    webp = ['52494646']
    wmv = ['3026b2758e66cf11']
    xml = ['3c3f786d6c2076657273696f6e3d22312e30223f3e']
    zip = ['504b0304', '504b0506', '504b0708', '504b4c495445', '504b537058', '57696e5a6970', '504b030414000100']


def determine_file(script_hex):
    for file_header in FileHeaders:
        for identifier in file_header.value:
            if identifier in script_hex:
                return file_header.name
    return ''

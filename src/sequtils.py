import zipfile
import hashlib
import uuid
import re
from datetime import datetime, UTC

def is_seq(path):
    return path.endswith(".zseq") or path.endswith(".seq") or path.endswith(".aseq")

def get_md5(path):
    with zipfile.ZipFile(path, 'r') as zip:
        seq_name = next((x for x in zip.namelist() if is_seq(x)), None)
        if(seq_name == None): raise Exception("SEQ NOT FOUND IN FILE " + path)
        with zip.open(seq_name) as seq:
            # Make sure to clamp the main volume to zero, to get a consistent hash
            data = bytearray(seq.read())
            for i in range(len(data) - 1):
                if data[i] == 0xDB: data[i + 1] = 0x00 #TODO: Implement proper seq reader, because songs with multiple main volume commands can trip up this system
            return hashlib.md5(data).hexdigest()

def get_current_date_string():
    return datetime.now(UTC).isoformat(timespec='milliseconds').replace("+00:00", "Z")

def get_uuid():
    return str(uuid.uuid4())

def get_safe_path(game, song):
    unsafe_characters = r'[\\\/:*?"<>|]'
    remove_trailing_dots = r'\.+$'
    return re.sub(remove_trailing_dots, "", re.sub(unsafe_characters, "", game)) + "/" + re.sub(unsafe_characters, "", song)

def is_cross_game_bank(bank: int, is_ootrs: bool) -> bool:
    if is_ootrs: return bank in ootrs_to_mm_bank_map.keys()
    else: return bank in ootrs_to_mm_bank_map.values()

ootrs_to_mm_bank_map = {
    0x03: 0x03, # Hyrule Field
    0x05: 0x04, # Market
    0x08: 0x05, # Kakariko (Guitar)
    0x09: 0x06, # Fairy Fountain
    0x0D: 0x07, # Lon Lon Ranch
    0x0E: 0x26, # Goron City
    0x11: 0x08, # Horse Race
    0x12: 0x09, # Warp Songs
    0x14: 0x0A, # Shooting Gallery
    0x15: 0x0B, # Zora's Domain
    0x16: 0x0C, # Shop
    0x1C: 0x0D, # Lakeside Laboratory
    0x1D: 0x0E, # Koume and Kotake
    0x23: 0x0F, # Fanfares
    0x24: 0x10 # Owl
}
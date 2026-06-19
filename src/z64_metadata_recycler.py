import argparse
import os
import json
import hashlib
import faulthandler
import zipfile

# Enable error line report
faulthandler.enable()

# First get the md5 and database of the source repo.
# Then, copy the metadata to any file that shares the same md5 in the target repo (that way we know they are copies)
# The metadata should be:
# - Game name
# - Song name (and update any filenames and paths)
# - Converters
# - Composers

def recicle_metadata(source_repo_path, target_repo_path):
    songs_path = 'z64packer/z64songs.json'
    source_database = []

    # Get the source database and get their md5 (readonly mode)
    os.chdir(source_repo_path)
    with open(songs_path, 'r', encoding='utf-8-sig') as database_file:
        properties = get_repo_properties(source_repo_path)
        database = json.load(database_file)
        for i, entry in enumerate(database):
            if isinstance(entry, dict):
                entry["md5"] = get_md5(os.path.join(properties["binaries"], entry["file"]))
                source_database.append(entry)

    # Now go to the target database and search any file that matches (read write mode)
    os.chdir(target_repo_path)
    with open(songs_path, 'r+', encoding='utf-8-sig') as database_file:
        properties = get_repo_properties(target_repo_path)
        database = json.load(database_file)
        for i, entry in enumerate(database):
            if isinstance(entry, dict):
                binary_path = os.path.join(properties["binaries"], entry["file"])
                hash = get_md5(binary_path)
                match = next((s for s in source_database if s["md5"] == hash), None)
                if match != None:
                    print(hash + " - Exact match found: " + match["file"] + " = " + entry["file"])

                    if match["song"] != entry["song"]:
                        if entry["song"] == "Armored Armadillo v2" or entry["song"] == "Megalomania v2" or entry["song"] == "Champion" or entry["song"] == "Fossil Falls v2" or entry["song"] == "Deku Palace V2" or entry["song"] == "Southern Swamp V2" or entry["song"] == "Stone Tower V2":
                            print("MANUAL CASE SKIPPED")
                            continue

                        print("Song name differences! Fixing...")

                        new_path = match["file"].replace(".mmrs", ".ootrs")
                        os.rename(binary_path, os.path.join(properties["binaries"], new_path))
                        database[i]["song"] = match["song"]
                        database[i]["file"] = new_path

                    # These are the most dangerous, so do them manually
                    if match["game"] != entry["game"]:
                        print("Game name differences! Skipping... ")
                        continue
                    
                    database[i]["converters"] = match["converters"]
                    database[i]["composers"] = match["composers"]
                    
                    

                    print("Fixed!")
        # Replace song database with this one
        database_file.seek(0)
        json.dump(database, database_file, indent=2, ensure_ascii=False)
        database_file.truncate()

    return True


def get_repo_properties(repo_path):
    properties_path = os.path.join(repo_path, 'z64packer/z64musicpacker.properties')
    if not os.path.exists(properties_path):
        raise Exception('This is not an Z64 repository | Missing z64musicpacker.properties file')
    with open(properties_path, encoding='utf-8-sig') as file:
        return json.load(file)

def get_md5(path):
    with zipfile.ZipFile(path, 'r') as zip:
        seq_name = next((x for x in zip.namelist() if is_seq(x)), None)
        if(seq_name == None): raise Exception("SEQ NOT FOUND IN FILE " + path)
        with zip.open(seq_name) as seq:
            # Make sure to clamp the main volume to zero, to get a consistent hash
            data = bytearray(seq.read())
            for i in range(len(data) - 1):
                if data[i] == 0xDB: data[i + 1] = 0x00
            return hashlib.md5(data).hexdigest()
        
def is_seq(path):
    return path.endswith(".zseq") or path.endswith(".seq") or path.endswith(".aseq")

if __name__ == '__main__':
    print("METADATA RECYCLER")
    parser = argparse.ArgumentParser(
        description="Extract metadata from one repository, and imports it to another when files match exactly (md5 comparison)."
    )
    parser.add_argument("--source_repo_path", help="The path to the z64packer repository from where we will extract metadata")
    parser.add_argument("--target_repo_path", help="The path to the z64packer repository to which we will import metadata")
    args = parser.parse_args()

    source_repo_path = args.source_repo_path
    target_repo_path = args.target_repo_path

    result = recicle_metadata(source_repo_path, target_repo_path)
    if result: print("Process completed succesfully!")
    else: print("An error occured")



# Database fixer for Z64 custom music repositories | Version 1.4
import json
import os
import re
import zipfile
import uuid
import traceback
import yaml
import argparse
from pathlib import Path
import faulthandler

# Enable error line report
faulthandler.enable()

def detectSongs(repo_path):
    # Move to the specified path... by default we use the current directory
    os.chdir(repo_path)

    propertiesPath = 'z64musicpacker.properties'
    binariesPath = 'z64packer/binaries.zip'
    songsPath = 'z64packer/z64songs.json'
    gamesPath = 'z64packer/z64games.json'

    if not os.path.exists(propertiesPath):
        propertiesPath = 'z64packer/z64musicpacker.properties'
        if not os.path.exists(propertiesPath):
            raise Exception('This is not an Z64 repository | Missing z64musicpacker.properties file')
    
    with open(propertiesPath, encoding='utf-8') as propertiesFile:
        properties = json.load(propertiesFile)
        binaries = properties['binaries']

        # Statistics
        valid_files = 0
        missing_files = 0
        bank_stuffing_files = 0
        empty_folders = 0

        # Pack all the files in a single zip, to provide a faster download in the web tool
        with zipfile.ZipFile(binariesPath, 'w', zipfile.ZIP_DEFLATED) as binariesZip:

            # Try to create necessary files
            if not os.path.exists(songsPath):
                with open(songsPath, 'w+') as f: f.write('[]')
            if not os.path.exists(gamesPath):
                with open(gamesPath, 'w+') as f: f.write('[]')

            # First, open the games database
            with open(gamesPath, 'r+', encoding='utf-8') as gamesFile:
                print("OPENING GAME DATABASE FILE")
                games = json.load(gamesFile)
                games = list(filter(lambda g: isinstance(g, dict), games))

                # Open the database, so we can modify it
                with open(songsPath, 'r+', encoding='utf-8') as databaseFile:
                    print("OPENING SONG DATABASE FILE")
                    database = json.load(databaseFile)
                    database = list(filter(lambda s: isinstance(s, dict), database))
                    
                    # First, check if the names and files are correct
                    # The database name has priority in this
                    for i, entry in enumerate(database):

                        # Check if the file is there...
                        actualPath = entry['file']
                        if os.path.isfile(os.path.join(binaries, actualPath)):
                            
                            # This commented snippet is for giving priority to the folder-file structure.
                            # But that's impossible, since if a file is renamed, we cannot know to which one it corresponds!
                            # So, if a file is renamed, it will be eliminated, and then readded.
                            # The only way to edit game names, is through the submission form, or directly inside the database.

                            # pathSplitted = actualPath.split('/')
                            # for index, pathSection in enumerate(reversed(pathSplitted)):
                            #    # Change the song name if the filename is different
                            #    if index == 0 and entry["song"] != Path(pathSection).stem:
                            #        print("DIFFERENT SONG NAME DETECTED")
                            #        print("Renaming: " + entry["song"] + " -> " + Path(pathSection).stem)
                            #        database[i]["song"] = Path(pathSection).stem
                            #    # Change the game name if the folder is different
                            #    if index == 1 and entry["game"] != pathSection:
                            #        print("DIFFERENT GAME NAME DETECTED")
                            #        print("Renaming: " + entry["game"] + " -> " + pathSection)
                            #        database[i]["game"] = pathSection
                            #    # Setup the series if there are a folder up this one
                            #    # else: games ...

                            # If the path in the folder structure, change it on the database!

                            # TODO: HERE WE NEED TO ADD THE SERIES TO THE INTENDED PATH

                            # print("Checking name")
                            valid_files += 1

                            #intendedPath = entry['game'] + '/' + entry['song'] + os.path.splitext(actualPath)[1]
                            #if intendedPath != actualPath:
                            #    print("DIFFERENT PATH DETECTED")
                            #    print("Renaming: " + entry["file"] + " -> " + intendedPath)

                            #    # Only rename it if we find it... It may have changed already!
                            #    database[i]['file'] = intendedPath
                            #    os.renames(os.path.join(binaries, actualPath), os.path.join(binaries, intendedPath))


                        
                        # If we don't find it, then remove it from the database
                        # Still, evaluate if this is ok to do...
                        else:
                            print('MISSING ENTRY DETECTED: ' + actualPath + " Removing...")
                            missing_files += 1
                            database.pop(i)


                    # Check every single file inside the binaries folder
                    songs = os.walk(binaries)
                    for dirpath, dirnames, filenames in songs:
                        directory = dirpath.replace(binaries, '')

                        # Remove empty folders
                        if len(os.listdir(dirpath)) == 0:
                            print("EMPTY FOLDER DETECTED: " + dirpath + " Clean up... Clean up...")
                            empty_folders += 1
                            os.rmdir(dirpath)
                            continue

                        # Check every single file inside this folder
                        for filename in filenames:
                            try:
                                # Only check ootrs and mmrs files
                                if not filename.endswith('.ootrs') and not filename.endswith('.mmrs'): continue

                                # Fix any bank stuffing to port it to using custom bank 28
                                path = os.path.join(dirpath, filename)
                                database_path = os.path.join(directory, filename).replace("\\","/")
                                if(filename.endswith('.mmrs')):
                                    bank_stuffing_files += fix_bank_stuffing(database, database_path, path)

                                # Extract data from the file
                                type, categories, usesCustomBank, usesCustomSamples, usesFormmask = extract_metadata(path)

                                # GAME MANAGEMENT
                                # Update the games database
                                directories = directory.replace("\\","/").split('/')
                                game = safe_list_get(directories, -1, "Unknown")

                                #series = safe_list_get(directories, -2, "") # We only go 1 up the directories... We don't support series in series
                                # We no longer support modifying the series trough here, that needs to be done manually
                                # Only done for new games trhu folder structure
                                # But the idea is to NOT use folder structure for this
                                #else:
                                #    print('Updating game to series: ' + series)
                                #    games[gameIndex]["series"] = series


                                # SONG MANAGEMENT
                                # Check if the file is in the database
                                # THIS COMPARISON NEEDS TO NOT CHECK FOR DOUBLE COLONS!
                                detectedInDatabase = any(x for x in database if path_comparison(x["file"], database_path))

                                # If the file is in the DB, instead check it's integrity
                                if detectedInDatabase:
                                    # print('Updating file on DB: ' + database_path)
                                    i = [x["file"] for x in database].index(database_path)
                                    database[i]["type"] = type
                                    database[i]["categories"] = categories
                                    database[i]["usesCustomBank"] = usesCustomBank
                                    database[i]["usesCustomSamples"] = usesCustomSamples
                                    database[i]["usesFormmask"] = usesFormmask

                                    # Update the game, so we are not creating duplicates
                                    game = database[i]["game"]

                                # If is not there, add it!
                                else:
                                    print('Adding missing file to DB: ' + database_path)
                                    database.append({
                                        'game': game,
                                        'song': filename.replace('.ootrs', '').replace('.mmrs', ''),
                                        'type': type,
                                        'categories': categories,
                                        'usesCustomBank': usesCustomBank,
                                        'usesCustomSamples': usesCustomSamples,
                                        'usesFormmask': usesFormmask,
                                        'uuid': str(uuid.uuid4()),
                                        'file': database_path
                                    })

                                # If it's not in the list, just add it
                                gameDetectedInDatabase = any(x for x in games if path_comparison(x["game"], game))
                                if not gameDetectedInDatabase:
                                    print('Adding missing game to DB: ' + game)
                                    games.append({
                                        "game": game
                                    })

                                # Add this file to the main zip
                                osPath = os.path.join(dirpath, filename)
                                binariesZip.write(osPath)

                            except Exception:
                                print("An error ocurred while processing the file " + filename + ": " + traceback.format_exc())
                    
                    # Add the sentinel lines at the end to prevent merge conflicts
                    sentinel_line = "__SENTINEL__: ONLY ADD ENTRIES ABOVE THIS LINE TO PREVENT MERGE CONFLICTS. Oh also, don't delete it please thank you <3"
                    database.append(sentinel_line)
                    games.append(sentinel_line)
                    
                    # Replace song database with this one
                    databaseFile.seek(0)
                    json.dump(database, databaseFile, indent=2, ensure_ascii=False)
                    databaseFile.truncate()

                 # Replace game database with this one
                gamesFile.seek(0)
                json.dump(games, gamesFile, indent=2, ensure_ascii=False)
                gamesFile.truncate()

        # Print our statistics
        print("Statistics:")
        print(f"Valid files: {valid_files}")
        print(f"Missing files: {missing_files}")
        print(f"Bank stuffing fixed: {bank_stuffing_files}")
        print(f"Empty folders removed: {empty_folders}")

    return True

def safe_list_get(list, idx, default):
  try:
    return list[idx]
  except IndexError:
    return default
  
def safe_list_index(iterable, value, default = None):
    for i, item in enumerate(iterable):
        if item == value:
            return i
    return default

def path_comparison(a, b):
    unsafeCharacters = r'[\\\/:*?"<>|]'
    return re.sub(unsafeCharacters, '', a).lower() == re.sub(unsafeCharacters, '', b).lower()

# ========= PROCESSING ==========

def extract_metadata(path) -> tuple[str, list, bool, bool, bool]:
    archive = zipfile.ZipFile(path, 'r')
    namelist = archive.namelist()
    
    isOOTRS = path.endswith('.ootrs')
    isUniversalYamlFormat = any(n.endswith('.metadata') for n in namelist)
    
    if isUniversalYamlFormat: return extract_metadata_from_universal_yaml_format(archive, namelist)
    elif isOOTRS: return extract_metadata_from_ootrs(archive, namelist)
    else: return extract_metadata_from_mmrs(archive, namelist)

def fix_bank_stuffing(database, database_path, path) -> bool:
    file_path, extension = os.path.splitext(path)
    path_of_original = None

    with zipfile.ZipFile(path, 'r') as zin:
        namelist = zin.namelist()

        # Count the amount of zseq files we have... if they are more than 1, we have bank stuffing
        seqs = [n for n in namelist if n.endswith('.zseq')]
        if len(seqs) > 1:
            print("BANK STUFFING: " + path + " Fixing...")

            # TODO: NO ALL CUSTOM BANK STUFFING IS MADE EQUAL
            # Earthbound Begginings/Hippy.mmrs has a custom bank seq + a chiptune set
            # Both of them are on bank 24, but the chiptune one is on bank 0x24
            # HOW can you know if bank stuffing will be an issue?

            # We need to know:
            # 1. WHY bank stuffing was necessary
            # 2. HOW to differentiate stuffing for CUSTOM BANKS, or for RANDOM TRACKS

            # Answers:
            # 1. It was necessary so the rando can properly add more instruments some unused banks (stuffing) so
            #    that songs could use them. Multiple banks were provided so that the rando has more chances to
            #    find space.
            # 2. If a zseq has a related custom bank, it means that set should be 28 in modern standards. Every other zseq
            #    with a related custom bank should be ommited. If a zseq doesn't have a related bank, it should be splitted.

            # TODO: Include behaviours to replace original track (prioritize custom bank) 
            # TODO: Copy entry to database for new splitted files!
            # TODO: 0x. or 1a or b tend to be the chiptune seqs...
            # TODO: If it has custom bank + another file, that other file is always an 8bit Version
            # TODO: There is ONE exception (Sheriff Domestic - Got Disk) so that we manage manually

            custom_bank_already_extracted = False
            for i, seq in enumerate(seqs):
                bank, _ = os.path.splitext(seq)
                print("Checking bank " + bank + "...")

                # Check if this seq has a custom bank attached
                is_custom_bank = any(n == f"{bank}.zbank" for n in namelist)
                if is_custom_bank:
                    # We extract only one seq with custom bank, because all of the other are copies
                    if custom_bank_already_extracted: continue

                    # Create a new file to store this seq, force 
                    custom_bank_already_extracted = True
                    splitted_path = f"{file_path} (Custom Bank){extension}"
                    extract_file_by_bank(zin, splitted_path, bank_to_keep=bank, set_bank="28")

                    # Always consider the custom bank as the original
                    path_of_original = splitted_path
                
                # If has no custom banks, this is a variant and we need to separate it
                else:
                    version = f" (Bank {bank})"
                    
                    # Some specific banks tend to be used for 8bit version of tracks
                    if bank.startswith("0x") or bank == "1a" or bank == "b":
                        version = " (8bit Version)"
                        
                    # Create a new file to store this seq
                    splitted_path = f"{file_path}{version}{extension}"
                    extract_file_by_bank(zin, splitted_path, bank_to_keep=bank)
                    
                    # If is not an 8bit version, set it to replace the original file
                    if not path_of_original and not "(8bit Version)" in splitted_path:
                        path_of_original = splitted_path
                    
                    # If we are creating a new file, then create a new entry in the database
                    else:
                        # Get the original entry to make a copy
                        print("ADDING NEW ENTRY TO DATABASE: " + splitted_path)
                        entry = next((x for x in database if path_comparison(x["file"], database_path)), None)
                        
                        # If we find it, then add it, but remove it's preview, since we do not have it yet
                        if entry:
                            database_path_root, database_path_ext = os.path.splitext(database_path)
                            copied_entry = dict(entry)
                            copied_entry["file"] = f"{database_path_root}{version}{database_path_ext}"
                            copied_entry["song"] = entry["song"] + version
                            copied_entry["preview"] = ""
                            database.append(copied_entry)

            
    # Replace the old file with the new one
    if path_of_original: os.replace(path_of_original, path)
    return path_of_original != None

def extract_file_by_bank(zin, new_file_path, set_bank = None, bank_to_keep = None):
    with zipfile.ZipFile(new_file_path, 'w') as zout:
        file_to_keep = bank_to_keep

        for item in zin.infolist():
            buffer = zin.read(item)
            root, extension = os.path.splitext(item.filename)
            if extension == '.zseq' or extension == '.zbank' or extension == '.bankmeta':

                # If this is the first file we find, we are gonna keep this bank set
                if not file_to_keep: file_to_keep = root

                # Skip this file if is not from the set
                if root != file_to_keep: continue

                # Replace the bank if needed
                if set_bank: item.filename = f"{set_bank}{extension}"
                    
            # If we are not duplicate, we just rewrite to the file
            zout.writestr(item.filename, buffer)


def extract_metadata_from_universal_yaml_format(archive, namelist) -> tuple[str, list, bool, bool, bool]:
    for name in namelist:
        if name.endswith('.metadata'):
            with archive.open(name) as metadata_file:
                metadata_yaml = yaml.safe_load(metadata_file.read())
                metadata = metadata_yaml['metadata']

                # These first two are not optional!
                seq_type = metadata['song type'].lower()
                groups = metadata['music groups']
                usesCustomBank = any(n.endswith('.zbank') for n in namelist)
                usesCustomSamples = any(n.endswith('.zsound') for n in namelist)
                usesFormmask = len(metadata.get('formmask', [])) > 0

                return seq_type, groups, usesCustomBank, usesCustomSamples, usesFormmask
    raise EOFError("Couldn't find yaml metadata in file!")


def extract_metadata_from_ootrs(archive, namelist) -> tuple[str, list, bool, bool, bool]:
    for name in namelist:
        if name.endswith('.meta'):
            with archive.open(name) as meta_file:
                lines = meta_file.readlines()
                lines = [line.decode('utf8').rstrip() for line in lines]

                # Extract the type and groups
                seq_type = (lines[2] if len(lines) >= 3 else 'bgm').lower()
                groups = [g.strip() for g in lines[3].split(',')] if len(lines) >= 4 else []

                # Check if uses custom banks and samples
                usesCustomBank = any(n.endswith('.zbank') for n in namelist)
                usesCustomSamples = any(n.endswith('.zsound') for n in namelist)

                return seq_type, groups, usesCustomBank, usesCustomSamples, False
    raise EOFError("Couldn't find ootrs metadata in file!")

mm_fanfare_categories = [
    "8", "9", "10",
    "122", "124", "137", "139", "13D", "13F", "141", "152", "155", "177",
    "119", "108", "109", "120", "121", "178", "179", "17E", "17C", "12B"
]
def extract_metadata_from_mmrs(archive, namelist) -> tuple[str, list, bool, bool, bool]:
    for name in namelist:
        if name == 'categories.txt':
            with archive.open(name) as categories_file:
                lines = categories_file.readlines()
                lines = [line.decode('utf8').rstrip() for line in lines]

                # Extract the categories
                categories = [g.strip() for g in lines[0].replace('-', ',').split(',')] if len(lines) >= 1 else []

                # Define the type by checking the categories
                isFanfare = all(cat in mm_fanfare_categories for cat in categories) and len(categories) > 0
                seq_type = 'fanfare' if isFanfare else 'bgm'

                # Check if uses custom banks and samples
                usesCustomBank = any(n.endswith('.zbank') for n in namelist)
                usesCustomSamples = any(n.endswith('.zsound') for n in namelist)
                usesFormmask = any(n.endswith('.formmask') for n in namelist)

                return seq_type, categories, usesCustomBank, usesCustomSamples, usesFormmask
    raise EOFError("Couldn't find mmrs metadata in file!")

    
if __name__ == '__main__':
    print("RUNNING Z64 DATABASE FIXER!")
    parser = argparse.ArgumentParser(
        description="Rebuilds the Z64 packer database, manages missing files, and fixes some known issues."
    )
    parser.add_argument("--repo_path", default=".", help="The path to the z64packer repository. By default is the current directory.")
    args = parser.parse_args()

    repo_path = args.repo_path

    result = detectSongs(repo_path)

    if result: print("Process completed succesfully!")
    else: print("An error occured")
    

import argparse
import os
import json
import faulthandler
import sequtils
import shutil
from pathlib import Path

# Enable error line report
faulthandler.enable()

def parse_messages(repo_path, releases_folder_path):
    os.chdir(repo_path)
    with open("z64packer/z64musicpacker.properties", encoding='utf-8-sig') as properties_file:
        properties = json.load(properties_file)
        binaries = properties['binaries']
        previews = properties['previews']

        with open("z64packer/z64songs.json", "r+", encoding='utf-8-sig') as database_file:
            database = json.load(database_file)
            database = list(filter(lambda s: isinstance(s, dict), database))
            creation_date = sequtils.get_current_date_string()

            # Get data from the database to help the process
            hashlist = []
            files = []
            for i, entry in enumerate(database):
                path = os.path.join(binaries, entry["file"])
                hashlist.append(sequtils.get_md5(path))
                files.append(entry["file"].lower())

            # Move to the releases folder, to process and stage all the new files
            file_count = 0
            duplicates = 0

            for dir in sorted([x for x in Path(releases_folder_path).iterdir() if x.is_dir()], key=lambda d: int(d.name)):
                entry = {}
                filename = ""
                absoulte_path = ""
                author = ""
                for file in dir.iterdir():
                    # Check if it has a file, and get it's metadata
                    if file.name.endswith(".mmrs"):
                        filename = file.name
                        absoulte_path = file.absolute()

                    if file.name.endswith(".txt"):
                        with open(file.absolute(), 'r', encoding='utf-8-sig') as message_file:
                            content = clean_content(message_file.read())
                            for line in content.split("\n"):
                                # Add TODOs to missing properties
                                if line.startswith("Author: "): author = extract_value(line).split(" (")[0]
                                if song_check(line) and not contains("Original Song:", line): entry["song"] = extract_value(line)
                                if contains("Game:", line) or contains("Game Name:", line) or contains("Source:", line): entry["game"] = extract_value(line)
                                if contains("Original Composer:", line): entry["composers"] = [extract_value(line)]
                                if contains("MMR File Written By:", line) or contains("MMR File Wriiten By:", line): entry["converters"] = [extract_value(line)]
                                # entry["message_id"] = dir.name

                # Continue the process only if we find a mmrs
                if filename:
                    file_count += 1

                    # Some finishing touches...
                    if not entry.get("song"): entry["song"] = filename.replace(".mmrs", "")
                    if not entry.get("game"): entry["game"] = "TODO"
                    if not "converters" in entry: entry["converters"] = [author]
                    entry["uuid"] = sequtils.get_uuid()
                    entry["hash"] = sequtils.get_md5(absoulte_path)
                    entry["creationDate"] = creation_date

                    # Skip the file if is already in the repo
                    if entry["hash"] in hashlist:
                        duplicates += 1
                        continue

                    # Create a valid file path and check if is not a dupe
                    file_path = sequtils.get_safe_path(entry["game"], entry["song"]) + ".mmrs"
                    if file_path.lower() in files:
                        #print(f"DUPE: {file_path}")
                        file_path = file_path.replace(".mmrs", "") + " [2].mmrs"
                        entry["song"] += " [2]"
                        #print(f"FIXED PATH: {file_path}")

                    print(f"ADDING: {file_path}")

                    # Copy the file to the actual destination
                    final_song_path = os.path.join(binaries, file_path)
                    os.makedirs(os.path.dirname(final_song_path), exist_ok=True)
                    shutil.copy(absoulte_path, final_song_path)
                      
                    # Now finish the entry and add it to database
                    entry["file"] = file_path
                    entry["preview"] = ""
                    database.append(entry)
                    files.append(file_path)

            # Finish up modifying the database
            sentinel_line = "__SENTINEL__: ONLY ADD ENTRIES ABOVE THIS LINE TO PREVENT MERGE CONFLICTS. Oh also, don't delete it please thank you <3"
            database.append(sentinel_line)
            database_file.seek(0)
            json.dump(database, database_file, indent=2, ensure_ascii=False)
            database_file.truncate()
                        
            print(f"Total files: {file_count}")
            print(f"Not in repo: {file_count - duplicates}")
            print(f"Already in repo: {duplicates}")


# ---------------------------------------------------------------

def contains(text, line) -> bool:
    return text.casefold() in line.casefold()

def extract_value(line):
    return line.split(":", 1)[1].strip()

def content_is_song_check(content):
    return song_check(content) or contains("Name:", content)

def song_check(line):
    return line.startswith("Name:") or contains("Song:", line) or contains("Song Name:", line) or contains("Song Names:", line) or contains("Title:", line) or contains("Sound:", line) or contains("Sound Name:", line) or contains("Track:", line)

def clean_content(content):
    return content.replace("**Game**", "Game:").replace("::", ":").replace("**", "").replace("||", "").replace("\r", "")

# ---------------------------------------------------------------

if __name__ == '__main__':
    print("TOTO BOT PARSER")
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo_path", default=".", help="The path to the z64packer repository. By default is the current directory.")
    parser.add_argument("--releases_folder_path", default=".", help="The path to the bot's extraction.")
    args = parser.parse_args()

    repo_path = args.repo_path
    releases_folder_path = args.releases_folder_path
    result = parse_messages(repo_path, releases_folder_path)

    if result: print("Process completed succesfully!")
    else: print("An error occured")
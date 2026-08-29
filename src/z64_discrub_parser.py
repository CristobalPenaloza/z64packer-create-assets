import argparse
import os
import json
import soundfile as sf
import loudness
import faulthandler
import re

# Enable error line report
faulthandler.enable()

def contains(text, line) -> bool:
    return text.casefold() in line.casefold()

def extract_value(line):
    return line.split(":", 1)[1].strip()

def content_is_song_check(content):
    return song_check(content) or contains("Name:", content)

def song_check(line):
    return line.startswith("Name:") or contains("Song:", line) or contains("Song Name:", line) or contains("Song Names:", line) or contains("Title:", line) or contains("Sound:", line) or contains("Sound Name:", line) or contains("Track:", line)

def clean_content(content):
    return content.replace("**Game**", "Game:").replace("**", "").replace("||", "").replace("\r", "")

# AI slopped to existence:
def get_highest_bracket_number(base_string, string_list):
    highest_num = -1
    
    # 1. Escape the base string for safety
    escaped_base = re.escape(base_string)
    
    # 2. Match exact base string OR base string with " (n)"
    pattern = re.compile(rf"^{escaped_base}(?: \((\d+)\))?$")
    
    for s in string_list:
        match = pattern.match(s)
        
        if match:
            if match.group(1):
                # Bracketed version found
                num = int(match.group(1))
                if num > highest_num:
                    highest_num = num
            else:
                # Exact match found without brackets
                if highest_num < 0:
                    highest_num = 0
                    
    return highest_num

def parse_messages(repo_path, releases_folder_path):

    # Open the repo database
    os.chdir(repo_path)
    with open("z64packer/z64songs.json", "r+", encoding='utf-8-sig') as database_file:
        database = json.load(database_file)

        # Move to the releases folder, to process and stage all the new files
        with open(os.path.join(releases_folder_path, "music_releases-page-1.json"), 'r+', encoding='utf-8-sig') as messages_file:
            print("OPENING RELEASES")
            messages = json.load(messages_file)
            skipped = []
            entries = []

            # Now enumerate all attachments, those are the most important ones!
            attachments = os.path.join(releases_folder_path, "media/attachments/")
            for filename in os.listdir(attachments):

                entry = {}
                i = int(filename.split("_")[0])

                # Get the actual song filename... do it this way since it can be bundled with preview files
                song_filename = ""
                for preview in messages[i]["attachments"]:
                    if preview["filename"].endswith(".mmrs") or preview["filename"].endswith(".zseq"):
                        # Check if we already registered this track
                        if not any(e for e in entries if e["index"] == i and e["discord_filename"] == preview["filename"]):
                            song_filename = preview["filename"]
                            break

                # Get the message content and the user who put it
                content = clean_content(messages[i]["content"])
                username = messages[i]["author"]["username"]

                # TODO: HANDLE ANY DUPLICATE NAMES, THOSE NEED TO BE APPENDED A NUMBER!!!
                
                # Make sure the content is not null
                # Go up by 5 messages backwards to find it, if necessary
                for retry in range(1, 10):
                    if not content_is_song_check(content):
                        # Only go back if the username matches
                        if username == messages[i - retry]["author"]["username"]:
                            content = clean_content(messages[i - retry]["content"])
                        else: break 

                # If the song STILL is not here, then skip it and notify, we need to add it manually
                if not content_is_song_check(content):
                    #print(">>>>> NO METADATA MESSAGE FOUND: " + song_filename)# + " | " + content)
                    #print(content)
                    skipped.append(f"{i} | {username} | {song_filename}")
                    continue

                for line in content.split("\n"):
                    # Add TODOs to missing properties
                    if song_check(line) and not contains("Original Song:", line): entry["song"] = extract_value(line)
                    if contains("Game:", line) or contains("Game Name:", line) or contains("Source:", line): entry["game"] = extract_value(line)
                    if contains("Original Composer:", line): entry["composers"] = [extract_value(line)]
                    if contains("MMR File Written By:", line) or contains("MMR File Wriiten By:", line): entry["converters"] = [extract_value(line)]

                # Add TODOs for missing properties
                if not "song" in entry: entry["song"] = "TODO"
                if not "game" in entry: entry["game"] = "TODO"

                # Add static parsing properties
                entry["index"] = i
                entry["discord_filename"] = song_filename

                # If this is a duplicate add as a variant " [n]"
                full_song_name = re.escape(f"{entry["game"]} - {entry["song"]}")
                dupe_pattern = re.compile(rf"^{full_song_name}(?: \[(\d+)\])?$")
                dupe_number = -1
                for full_name in [f"{e["game"]} - {e["song"]}" for e in entries]:
                    match = dupe_pattern.match(full_name)
                    if match:
                        if match.group(1):
                            # Bracketed version found
                            num = int(match.group(1))
                            if num > dupe_number: dupe_number = num
                        else:
                            # Exact match found without brackets
                            if dupe_number < 0: dupe_number = 0

                # print(dupe_number)
                if dupe_number >= 0:
                    entry["song"] = f"{entry["song"]} [{dupe_number + 1}]"

                # Add our entry
                #if entry["song"].startswith("TODO") or entry["game"].startswith("TODO"): 
                #    print(entry)

                entries.append(entry)
                print(entry)
            

            for i, message in enumerate(messages):
                entry = {}
                content = message["content"].replace("**", "").replace("||", "").replace("\r", "").split("\n")
                for line in content:
                    if ("Song: " in line or "Song Name: " in line) and not "Original Song: " in line: entry["song"] = line.split(": ", 1)[1]
                    if "Game: " in line or "Game Name: " in line: entry["game"] = line.split(": ", 1)[1]
                    entry["converter"] = message["author"]["global_name"]

                # print(entry)
    return True

    

if __name__ == '__main__':
    print("DISCRUB PARSER")

    parser = argparse.ArgumentParser()
    parser.add_argument("--repo_path", default=".", help="The path to the z64packer repository. By default is the current directory.")
    parser.add_argument("--releases_folder_path", default=".", help="The path to the Discrub extraction.")
    args = parser.parse_args()

    repo_path = args.repo_path
    releases_folder_path = args.releases_folder_path
    result = parse_messages(repo_path, releases_folder_path)

    if result: print("Process completed succesfully!")
    else: print("An error occured")
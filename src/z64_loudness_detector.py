import argparse
import os
import json
import soundfile as sf
import loudness
import faulthandler

# Enable error line report
faulthandler.enable()

def detectSongs(repo_path, music_folder_path):
    # Move to the specified path... by default we use the current directory
    os.chdir(repo_path)

    # Open the database, so we can modify it
    songsPath = 'z64packer/z64songs.json'
    with open(songsPath, 'r+', encoding='utf-8-sig') as databaseFile:
        print("OPENING SONG DATABASE FILE")
        database = json.load(databaseFile)
        database = list(filter(lambda s: isinstance(s, dict), database))

        # Loop all entries
        for i, entry in enumerate(database):
            preview_filename = entry['game'].replace(":", "") + " - " + entry['song'].replace(":", "") + ".mp3"
            preview_path = os.path.join(music_folder_path, preview_filename)

            if os.path.exists(preview_path):
                print(f"Detected: {preview_filename}")
                audio, sr = sf.read(preview_path, dtype="float32")
                lufs = loudness.integrated_loudness(audio, sr)
                database[i]["lufs"] = f"{lufs:.2f}"
                print(f"Integrated Loudness: {lufs:.2f} LUFS")

            else:
                print(f"NOT FOUND: {preview_filename}")

        # Replace song database with this one
        databaseFile.seek(0)
        json.dump(database, databaseFile, indent=2, ensure_ascii=False)
        databaseFile.truncate()

    return True

    

if __name__ == '__main__':
    print("SEARCHING LOUDNESS")

    parser = argparse.ArgumentParser(
        description="Receives a music path, and sets the loudness of each detected file for the Z64 packer database."
    )
    parser.add_argument("--repo_path", default=".", help="The path to the z64packer repository. By default is the current directory.")
    parser.add_argument("--music_folder_path", default=".", help="The path to a music folder. The songs need to be recorded from BizHawk, so that they have the real loudness.")
    args = parser.parse_args()

    repo_path = args.repo_path
    music_folder_path = args.music_folder_path
    result = detectSongs(repo_path, music_folder_path)

    if result: print("Process completed succesfully!")
    else: print("An error occured")
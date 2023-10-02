import os
import concurrent.futures
import argparse
import json
import hashlib
from tqdm import tqdm

def find_dat_files(directory_path):
    """
    Find all .DAT files in a directory.

    Parameters:
    - directory_path (str): Path to the directory to search.

    Returns:
    - list: List of relative paths of .DAT files found in the directory.
    """
    dat_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.DAT'):
                relative_path = os.path.relpath(os.path.join(root, file), directory_path)
                dat_files.append(relative_path)
    return dat_files

def compute_file_hash(file_path, hash_function=hashlib.sha256):
    """
    Compute the hash of a file's content.

    Parameters:
    - file_path (str): Path to the file to hash.
    - hash_function (callable, optional): Hash function to use. Defaults to hashlib.sha256.

    Returns:
    - str: Hex digest of the file's hash.
    """
    hash_obj = hash_function()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def compare_files(relative_path, root_a, root_b, hash_cache, pbar):
    """
    Compare two files based on their hash values.

    Parameters:
    - relative_path (str): Relative path of the file to compare.
    - root_a (str): Root directory for the first file.
    - root_b (str): Root directory for the second file.
    - hash_cache (dict): Cache of previously computed file hashes.
    - pbar (tqdm.tqdm): Progress bar instance.

    Returns:
    - bool: True if the files have different content, otherwise False.
    """
    file_a = os.path.join(root_a, relative_path)
    file_b = os.path.join(root_b, relative_path)

    # Check modification times
    mod_time_a = os.path.getmtime(file_a)
    mod_time_b = os.path.getmtime(file_b)

    # If modification times are the same, files are considered identical without hashing
    if mod_time_a == mod_time_b:
        pbar.update(1)
        return False

    # Use cached hash if file hasn't been modified
    hash_a = hash_cache.get(file_a, {}).get('hash') if hash_cache.get(file_a, {}).get('mod_time', 0) >= mod_time_a else None
    hash_b = hash_cache.get(file_b, {}).get('hash') if hash_cache.get(file_b, {}).get('mod_time', 0) >= mod_time_b else None

    if not hash_a:
        hash_a = compute_file_hash(file_a)
        hash_cache[file_a] = {'hash': hash_a, 'mod_time': mod_time_a}
    if not hash_b:
        hash_b = compute_file_hash(file_b)
        hash_cache[file_b] = {'hash': hash_b, 'mod_time': mod_time_b}

    pbar.update(1)
    return hash_a != hash_b

def compare_directories(root_a, root_b, max_workers=None, cache_file="hash_cache.json"):
    """
    Compare .DAT files between two directories using a hash cache.

    Parameters:
    - root_a (str): Path to the first root directory.
    - root_b (str): Path to the second root directory.
    - max_workers (int, optional): Maximum number of threads to use. Defaults to None, which lets `ThreadPoolExecutor` decide.
    - cache_file (str, optional): Path to the hash cache file. Defaults to "hash_cache.json".

    Returns:
    - list: List of relative paths of .DAT files that have different content in the two directories.
    """
    # Load hash cache
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            hash_cache = json.load(f)
    else:
        hash_cache = {}

    dat_files_a = set(find_dat_files(root_a))
    dat_files_b = set(find_dat_files(root_b))
    common_files = dat_files_a.intersection(dat_files_b)
    differences = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=len(common_files), desc="Comparing files", unit="file") as pbar:
            futures = {executor.submit(compare_files, relative_path, root_a, root_b, hash_cache, pbar): relative_path for relative_path in common_files}
            for future in concurrent.futures.as_completed(futures):
                relative_path = futures[future]
                if future.result():
                    differences.append(relative_path)

    # Save updated hash cache
    with open(cache_file, 'w') as f:
        json.dump(hash_cache, f)

    return differences

def main():
    """
    Entry point for the script. Parses command-line arguments, compares .DAT files between two directories,
    and either prints the results to the console or saves them to an output file.
    """
    parser = argparse.ArgumentParser(description="Compare .DAT files between two directory structures based on their relative paths and names.")
    parser.add_argument("dir1", help="Path to the first root directory")
    parser.add_argument("dir2", help="Path to the second root directory")
    parser.add_argument("-o", "--output", help="Path to the output file where results will be saved. If not provided, results are printed to the console.")
    args = parser.parse_args()

    different_files = compare_directories(args.dir1, args.dir2)

    if args.output:
        with open(args.output, 'w') as outfile:
            for f in different_files:
                outfile.write(f + "\n")
        print(f"Results saved to {args.output}")
    else:
        if different_files:
            print("Files with different content:")
            for f in different_files:
                print(f)
        else:
            print("All common .DAT files have identical content.")

if __name__ == "__main__":
    main()

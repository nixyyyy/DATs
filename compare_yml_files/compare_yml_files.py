import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

import argparse
import hashlib
import difflib
import shutil
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

def file_hash(filepath):
    """
    Compute the MD5 hash of a file.

    Args:
        filepath (str): The path to the file.

    Returns:
        str: The MD5 hash of the file.
    """
    hash_obj = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def detailed_comparison(file1, file2):
    """
    Compare the contents of two text files and generate a unified diff.

    Args:
        file1 (str): Path to the first text file.
        file2 (str): Path to the second text file.

    Returns:
        list: A list of strings representing the unified diff.
    """
    #Replaces invalid encoding bytes with the Unicode replacement character (U+FFFD)
    with open(file1, 'r', encoding='utf-8', errors='replace') as f1, open(file2, 'r', encoding='utf-8', errors='replace') as f2:
        diff = list(difflib.unified_diff(f1.readlines(), f2.readlines(), file1, file2))
    return diff

def compare_file_pair(args):
    """
    Compare two files and write the differences to an output file.

    Args:
        args (tuple): A tuple containing file paths and directories:
            file1 (str): Path to the first file.
            file2 (str): Path to the second file.
            output_dir (str): Directory where difference files are saved.
            dir1 (str): Base directory of file1.
    """
    file1, file2, output_dir, dir1 = args
    logger.info(f'Comparing {file1} and {file2}')
    hash1 = file_hash(file1)
    hash2 = file_hash(file2)

    if hash1 != hash2:
        diff = detailed_comparison(file1, file2)
        if diff:
            logger.info(f'Difference found between {file1} and {file2}')
            relative_path = os.path.relpath(file1, dir1)
            diff_output_path = os.path.join(output_dir, relative_path + ".diff")
            os.makedirs(os.path.dirname(diff_output_path), exist_ok=True)
            # Replaces invalid encoding bytes with the Unicode replacement character (U+FFFD)
            with open(diff_output_path, 'w', encoding='utf-8', errors='replace') as diff_file:
                for line in diff:
                    diff_file.write(line)

def clear_directory(directory):
    """
    Recursively delete all files and folders in the specified directory.

    Args:
        directory (str): The path to the directory to be cleared.
    """
    if os.path.exists(directory):
        # Prompt the user for confirmation before clearing the directory
        user_input = input(f"Do you want to clear the directory '{directory}'? (y/n): ").strip().lower()
        if user_input == 'y':
            shutil.rmtree(directory)
            print(f"Directory '{directory}' cleared.")
        else:
            print(f"Directory '{directory}' was not cleared.")

def diff_directories(dir1, dir2, output_dir):
    """
    Compare two directories with a similar structure and generate difference files.

    Args:
        dir1 (str): Path to the first directory.
        dir2 (str): Path to the second directory.
        output_dir (str): Directory where difference files will be written.
    """
    # Clear the output directory before starting
    clear_directory(output_dir)

    all_files_dir1 = [os.path.join(root, f) for root, _, files in os.walk(dir1) for f in files]
    all_files_dir2 = [f.replace(dir1, dir2) for f in all_files_dir1]

    total_files = len(all_files_dir1)

    with tqdm(total=total_files, desc="Processing files") as pbar:
        with ProcessPoolExecutor(max_workers=os.cpu_count() - 1) as executor:
            for _ in executor.map(compare_file_pair, zip(all_files_dir1, all_files_dir2, [output_dir] * total_files, [dir1] * total_files)):
                pbar.update(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Diff files in two directories with a similar structure.")
    parser.add_argument("dir1", help="Path to the first directory")
    parser.add_argument("dir2", help="Path to the second directory")
    parser.add_argument("output_dir", help="Directory where difference files will be written. Each file will be named based on its relative path and filename with a .diff extension.")

    args = parser.parse_args()

    diff_directories(args.dir1, args.dir2, args.output_dir)

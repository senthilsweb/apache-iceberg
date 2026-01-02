#!/usr/bin/env python3
"""
File Name: utils.py
Author: Senthilnathan Karuppaiah
Date: 02-JAN-2026
Description: 
Utility functions for the CSV to Iceberg loader.
Handles source detection, remote downloads, glob patterns, and table name resolution.
"""

import os
import re
import glob
import tempfile
import requests
from urllib.parse import urlparse, unquote
from pathlib import Path
from typing import List, Tuple, Optional


def is_remote_url(path: str) -> bool:
    """
    Check if path is a remote HTTP/HTTPS URL.
    
    Args:
        path: Path string to check
        
    Returns:
        True if path is a remote URL
    """
    if not path:
        return False
    return path.lower().startswith(('http://', 'https://'))


def is_glob_pattern(path: str) -> bool:
    """
    Check if path contains glob wildcards.
    
    Args:
        path: Path string to check
        
    Returns:
        True if path contains glob patterns
    """
    return any(char in path for char in ['*', '?', '[', ']'])


def resolve_table_name(filename: str, pluralize: bool = False) -> str:
    """
    Convert filename to a valid table name.
    Rules:
    - Remove file extension
    - Convert to lowercase
    - Replace spaces, hyphens, dots with underscores
    - Remove special characters
    - Optionally pluralize
    
    Args:
        filename: Original filename (e.g., "Sales Data.csv")
        pluralize: Whether to pluralize the name
        
    Returns:
        Valid table name (e.g., "sales_data" or "sales_datas")
    """
    # Get basename without path
    name = os.path.basename(filename)
    
    # Remove extension
    name = os.path.splitext(name)[0]
    
    # Convert to lowercase
    name = name.lower()
    
    # Replace spaces, hyphens, dots with underscores
    name = re.sub(r'[\s\-\.]+', '_', name)
    
    # Remove any non-alphanumeric characters except underscores
    name = re.sub(r'[^a-z0-9_]', '', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    # Remove consecutive underscores
    name = re.sub(r'_+', '_', name)
    
    # Pluralize if requested
    if pluralize and name:
        name = pluralize_word(name)
    
    return name


def pluralize_word(word: str) -> str:
    """
    Simple pluralization rules for English words.
    
    Args:
        word: Word to pluralize
        
    Returns:
        Pluralized word
    """
    if not word:
        return word
    
    # Words ending in 's', 'x', 'z', 'ch', 'sh' - add 'es'
    if word.endswith(('s', 'x', 'z', 'ch', 'sh')):
        return word + 'es'
    
    # Words ending in consonant + 'y' - replace 'y' with 'ies'
    if word.endswith('y') and len(word) > 1 and word[-2] not in 'aeiou':
        return word[:-1] + 'ies'
    
    # Words ending in 'f' or 'fe' - replace with 'ves'
    if word.endswith('fe'):
        return word[:-2] + 'ves'
    if word.endswith('f'):
        return word[:-1] + 'ves'
    
    # Default - add 's'
    return word + 's'


def download_remote_csv(url: str, temp_dir: Optional[str] = None) -> Tuple[str, str]:
    """
    Download a remote CSV file to a temporary location.
    
    Args:
        url: Remote URL to download
        temp_dir: Optional temp directory (uses system temp if not provided)
        
    Returns:
        Tuple of (local_file_path, original_filename)
        
    Raises:
        Exception if download fails
    """
    # Parse URL to get filename
    parsed = urlparse(url)
    path = unquote(parsed.path)
    original_filename = os.path.basename(path) or 'downloaded.csv'
    
    # Ensure .csv extension
    if not original_filename.lower().endswith('.csv'):
        original_filename += '.csv'
    
    # Create temp directory if needed
    if temp_dir is None:
        temp_dir = tempfile.mkdtemp(prefix='iceberg_loader_')
    
    local_path = os.path.join(temp_dir, original_filename)
    
    # Download file
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    return local_path, original_filename


def expand_glob_pattern(directory: str, pattern: str) -> List[str]:
    """
    Expand a glob pattern in a directory to get matching files.
    
    Args:
        directory: Base directory path
        pattern: Glob pattern (e.g., "*.csv", "sales_*.csv")
        
    Returns:
        List of matching file paths (absolute)
    """
    # Ensure directory ends with separator
    directory = os.path.abspath(directory)
    
    # Combine directory and pattern
    full_pattern = os.path.join(directory, pattern)
    
    # Find matching files
    matches = glob.glob(full_pattern)
    
    # Filter to only files (not directories)
    files = [f for f in matches if os.path.isfile(f)]
    
    # Sort for consistent ordering
    return sorted(files)


def detect_source_type(source_path: str, glob_pattern: Optional[str] = None) -> str:
    """
    Detect the type of source.
    
    Args:
        source_path: The source path (URL, file, or directory)
        glob_pattern: Optional glob pattern for directory sources
        
    Returns:
        One of: 'remote', 'glob', 'file'
    """
    if is_remote_url(source_path):
        return 'remote'
    
    if os.path.isdir(source_path) and glob_pattern:
        return 'glob'
    
    if os.path.isfile(source_path):
        return 'file'
    
    # Check if it's a glob pattern itself
    if is_glob_pattern(source_path):
        return 'glob'
    
    return 'unknown'


def get_files_to_process(source_path: str, glob_pattern: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Get list of files to process based on source configuration.
    
    Args:
        source_path: Source path (URL, file, or directory)
        glob_pattern: Optional glob pattern for directory sources
        
    Returns:
        List of tuples: (file_path, original_filename)
        For remote files, downloads first and returns temp path.
    """
    source_type = detect_source_type(source_path, glob_pattern)
    
    if source_type == 'remote':
        # Download and return single file
        local_path, filename = download_remote_csv(source_path)
        return [(local_path, filename)]
    
    elif source_type == 'glob':
        # Expand glob pattern
        if os.path.isdir(source_path):
            files = expand_glob_pattern(source_path, glob_pattern or '*.csv')
        else:
            # source_path itself is a glob pattern
            files = glob.glob(source_path)
            files = [f for f in files if os.path.isfile(f)]
        
        return [(f, os.path.basename(f)) for f in sorted(files)]
    
    elif source_type == 'file':
        # Single file
        return [(source_path, os.path.basename(source_path))]
    
    else:
        return []


def cleanup_temp_files(file_paths: List[str]) -> None:
    """
    Clean up temporary downloaded files.
    
    Args:
        file_paths: List of file paths to remove
    """
    for path in file_paths:
        try:
            if path and os.path.exists(path) and '/tmp/' in path or 'iceberg_loader_' in path:
                os.remove(path)
        except Exception:
            pass  # Ignore cleanup errors

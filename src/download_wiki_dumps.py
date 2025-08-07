#!/usr/bin/env python3
"""
Wikipedia Dump Downloader
Downloads article dumps for all Wikipedia languages from dumps.wikimedia.org
Respects 3-connection limit and skips already downloaded files.
"""

import os
import re
import sys
import time
import json
import glob
import threading
import queue
import argparse
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Constants
DEFAULT_TARGET_DIR = "../dumps"
MAX_CONNECTIONS = 3
CHUNK_SIZE = 8192 * 16  # 128KB chunks
TIMEOUT = 30
MAX_RETRIES = 3
USER_AGENT = "WikipediaDumpDownloader/1.0 (https://github.com/user/wiki-downloader)"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wiki_dump_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DownloadManager:
    """Manages concurrent downloads with rate limiting."""
    
    def __init__(self, max_connections=3):
        self.max_connections = max_connections
        self.active_downloads = {}
        self.completed_downloads = []
        self.failed_downloads = []
        self.lock = threading.Lock()
        self.total_bytes_downloaded = 0
        self.start_time = time.time()
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({'User-Agent': USER_AGENT})
    
    def download_file(self, url, dest_path, lang_code):
        """Download a file with progress tracking and resume support."""
        try:
            # Check if file already exists
            if os.path.exists(dest_path):
                local_size = os.path.getsize(dest_path)
                logger.info(f"[{lang_code}] File exists with size {self.format_bytes(local_size)}, verifying...")
                
                # Check remote size
                response = self.session.head(url, timeout=TIMEOUT)
                remote_size = int(response.headers.get('content-length', 0))
                
                if local_size == remote_size:
                    logger.info(f"[{lang_code}] File complete, skipping")
                    return True
                elif local_size < remote_size:
                    logger.info(f"[{lang_code}] Resuming download from {self.format_bytes(local_size)}")
                    headers = {'Range': f'bytes={local_size}-'}
                    mode = 'ab'
                    resume_pos = local_size
                else:
                    logger.warning(f"[{lang_code}] Local file larger than remote, redownloading")
                    mode = 'wb'
                    headers = {}
                    resume_pos = 0
            else:
                headers = {}
                mode = 'wb'
                resume_pos = 0
            
            # Start download
            response = self.session.get(url, headers=headers, stream=True, timeout=TIMEOUT)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0)) + resume_pos
            downloaded = resume_pos
            
            # Track active download
            with self.lock:
                self.active_downloads[lang_code] = {
                    'url': url,
                    'total_size': total_size,
                    'downloaded': downloaded,
                    'start_time': time.time()
                }
            
            # Download with progress
            with open(dest_path, mode) as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        with self.lock:
                            self.active_downloads[lang_code]['downloaded'] = downloaded
                            self.total_bytes_downloaded += len(chunk)
            
            # Mark as complete
            with self.lock:
                del self.active_downloads[lang_code]
                self.completed_downloads.append(lang_code)
            
            logger.info(f"[{lang_code}] Download complete: {self.format_bytes(total_size)}")
            return True
            
        except Exception as e:
            logger.error(f"[{lang_code}] Download failed: {str(e)}")
            with self.lock:
                if lang_code in self.active_downloads:
                    del self.active_downloads[lang_code]
                self.failed_downloads.append((lang_code, str(e)))
            return False
    
    def format_bytes(self, bytes_val):
        """Format bytes to human readable string."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
    
    def get_progress_string(self):
        """Get current progress status string."""
        with self.lock:
            active_count = len(self.active_downloads)
            completed_count = len(self.completed_downloads)
            failed_count = len(self.failed_downloads)
            
            elapsed = time.time() - self.start_time
            speed = self.total_bytes_downloaded / elapsed if elapsed > 0 else 0
            
            status_lines = []
            status_lines.append(f"\n{'='*60}")
            status_lines.append(f"Progress: {completed_count} completed, {failed_count} failed, {active_count} active")
            status_lines.append(f"Total downloaded: {self.format_bytes(self.total_bytes_downloaded)}")
            status_lines.append(f"Average speed: {self.format_bytes(speed)}/s")
            
            if self.active_downloads:
                status_lines.append("\nActive downloads:")
                for lang, info in self.active_downloads.items():
                    progress = (info['downloaded'] / info['total_size'] * 100) if info['total_size'] > 0 else 0
                    status_lines.append(f"  [{lang}] {progress:.1f}% of {self.format_bytes(info['total_size'])}")
            
            return '\n'.join(status_lines)


def get_all_wikipedia_languages():
    """Scrape Wikipedia languages from meta.wikimedia.org."""
    try:
        url = "https://meta.wikimedia.org/wiki/List_of_Wikipedias"
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Extract language codes from the page
        # Looking for patterns like "en.wikipedia.org", "de.wikipedia.org", etc.
        pattern = r'([a-z\-]+)\.wikipedia\.org'
        languages = re.findall(pattern, response.text)
        
        # Remove duplicates and sort
        languages = sorted(list(set(languages)))
        
        # Filter out obvious non-language codes
        filtered = []
        for lang in languages:
            # Skip if contains common non-language patterns
            if not any(x in lang for x in ['wiki', 'www', 'meta', 'commons', 'species', 'data']):
                filtered.append(lang)
        
        logger.info(f"Found {len(filtered)} Wikipedia languages")
        return filtered
        
    except Exception as e:
        logger.error(f"Failed to get Wikipedia languages: {str(e)}")
        # Fallback to a hardcoded list of major languages
        return ['en', 'de', 'fr', 'es', 'it', 'pt', 'ru', 'ja', 'zh', 'ar', 
                'nl', 'pl', 'sv', 'ceb', 'war', 'vi', 'uk', 'ca', 'no', 'fi',
                'cs', 'hu', 'ko', 'id', 'tr', 'ro', 'fa', 'sh', 'sr', 'ms',
                'eo', 'bg', 'da', 'he', 'lt', 'sk', 'sl', 'eu', 'et', 'hr',
                'simple']


def get_existing_dumps(target_dir):
    """Check which Wikipedia dumps are already downloaded."""
    existing = set()
    
    if not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)
        return existing
    
    # Look for dump files
    pattern = os.path.join(target_dir, "*wiki-*-pages-articles*.bz2")
    files = glob.glob(pattern)
    
    for filepath in files:
        filename = os.path.basename(filepath)
        # Extract language code from filename
        match = re.match(r'^([a-z\-]+)wiki-', filename)
        if match:
            lang_code = match.group(1)
            existing.add(lang_code)
            logger.info(f"Found existing dump for {lang_code}: {filename}")
    
    # Also check for partial downloads
    partial_pattern = os.path.join(target_dir, "*wiki-*-pages-articles*.bz2.download")
    partial_dirs = glob.glob(partial_pattern)
    for partial in partial_dirs:
        logger.info(f"Found partial download: {os.path.basename(partial)}")
    
    return existing


def find_available_dump(lang_code, session=None, retry_count=0, request_delay=2):
    """Find the latest available dump URL for a language."""
    if session is None:
        session = requests.Session()
        session.headers.update({'User-Agent': USER_AGENT})
    
    base_url = f"https://dumps.wikimedia.org/{lang_code}wiki/"
    
    try:
        # Add delay to avoid rate limiting
        if retry_count > 0:
            delay = min(60, request_delay * (2 ** retry_count))  # Exponential backoff
            logger.info(f"Waiting {delay}s before retry for {lang_code}...")
            time.sleep(delay)
        else:
            time.sleep(request_delay)  # Standard delay between requests
        
        # Get the main page for this wiki
        response = session.get(base_url, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Find all date directories (format: YYYYMMDD)
        date_pattern = r'href="(\d{8})/"'
        dates = re.findall(date_pattern, response.text)
        dates = sorted(dates, reverse=True)  # Most recent first
        
        if not dates:
            logger.warning(f"No dump dates found for {lang_code}")
            return None
        
        # Try each date until we find a complete dump
        for date in dates[:5]:  # Check last 5 dumps max
            dump_url = urljoin(base_url, f"{date}/")
            
            try:
                response = session.get(dump_url, timeout=TIMEOUT)
                response.raise_for_status()
                
                # Look for the pages-articles-multistream file
                # Pattern: langwiki-date-pages-articles-multistream.xml.bz2
                file_pattern = f'{lang_code}wiki-{date}-pages-articles-multistream\\.xml\\.bz2'
                
                if re.search(file_pattern, response.text):
                    # Check if dump is complete (not in progress)
                    if 'in-progress' not in response.text.lower() or date != dates[0]:
                        file_url = urljoin(dump_url, f"{lang_code}wiki-{date}-pages-articles-multistream.xml.bz2")
                        
                        # Verify the file exists
                        head_response = session.head(file_url, timeout=TIMEOUT)
                        if head_response.status_code == 200:
                            size = int(head_response.headers.get('content-length', 0))
                            logger.info(f"Found dump for {lang_code}: {date} ({size / 1024 / 1024:.1f} MB)")
                            return file_url
                        
            except Exception as e:
                logger.debug(f"Error checking {lang_code} dump {date}: {str(e)}")
                continue
        
        logger.warning(f"No complete dump found for {lang_code}")
        return None
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 503 and retry_count < 3:
            logger.warning(f"Got 503 for {lang_code}, retrying ({retry_count + 1}/3)...")
            return find_available_dump(lang_code, session, retry_count + 1, request_delay)
        else:
            logger.error(f"Failed to find dump for {lang_code}: {str(e)}")
            return None
    except Exception as e:
        logger.error(f"Failed to find dump for {lang_code}: {str(e)}")
        return None


def download_wikipedia_dumps(languages=None, target_dir=DEFAULT_TARGET_DIR, dry_run=False, max_connections=MAX_CONNECTIONS, request_delay=2):
    """Main function to download all Wikipedia dumps."""
    
    # Get list of all Wikipedia languages if not provided
    if languages is None:
        logger.info("Fetching list of all Wikipedia languages...")
        languages = get_all_wikipedia_languages()
    
    # Check existing dumps
    logger.info(f"Checking existing dumps in {target_dir}...")
    existing = get_existing_dumps(target_dir)
    
    # Filter out already downloaded languages
    to_download = [lang for lang in languages if lang not in existing]
    
    logger.info(f"Total languages: {len(languages)}")
    logger.info(f"Already downloaded: {len(existing)}")
    logger.info(f"To download: {len(to_download)}")
    
    if not to_download:
        logger.info("All dumps already downloaded!")
        return
    
    if dry_run:
        logger.info("DRY RUN - Would download the following languages:")
        for lang in to_download[:20]:  # Show first 20
            logger.info(f"  - {lang}")
        if len(to_download) > 20:
            logger.info(f"  ... and {len(to_download) - 20} more")
        return
    
    # Initialize download manager
    manager = DownloadManager(max_connections=max_connections)
    
    # First, discover all dump URLs sequentially to avoid overwhelming the server
    logger.info("Discovering available dump URLs...")
    dump_urls = {}
    
    for i, lang_code in enumerate(to_download, 1):
        logger.info(f"[{i}/{len(to_download)}] Finding dump URL for {lang_code}...")
        dump_url = find_available_dump(lang_code, manager.session, request_delay=request_delay)
        
        if dump_url:
            dump_urls[lang_code] = dump_url
            logger.info(f"Found dump for {lang_code}")
        else:
            logger.warning(f"No dump URL found for {lang_code}, skipping")
            manager.failed_downloads.append((lang_code, "No dump URL found"))
        
        # Print progress every 10 languages
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(to_download)} languages checked, {len(dump_urls)} dumps found")
    
    logger.info(f"Found {len(dump_urls)} available dumps to download")
    
    # Now process downloads with thread pool
    with ThreadPoolExecutor(max_workers=max_connections) as executor:
        futures = []
        
        for lang_code, dump_url in dump_urls.items():
            # Prepare destination path
            filename = os.path.basename(dump_url)
            dest_path = os.path.join(target_dir, filename)
            
            # Submit download task
            future = executor.submit(manager.download_file, dump_url, dest_path, lang_code)
            futures.append((future, lang_code))
            
            # Print progress periodically
            if len(futures) % 10 == 0:
                print(manager.get_progress_string())
        
        # Wait for all downloads to complete
        logger.info("Waiting for downloads to complete...")
        for future, lang_code in futures:
            try:
                future.result(timeout=3600)  # 1 hour timeout per download
            except Exception as e:
                logger.error(f"Download failed for {lang_code}: {str(e)}")
    
    # Final summary
    print(manager.get_progress_string())
    logger.info("\nDownload summary:")
    logger.info(f"  Completed: {len(manager.completed_downloads)}")
    logger.info(f"  Failed: {len(manager.failed_downloads)}")
    logger.info(f"  Total data: {manager.format_bytes(manager.total_bytes_downloaded)}")
    
    if manager.failed_downloads:
        logger.info("\nFailed downloads:")
        for lang, error in manager.failed_downloads:
            logger.info(f"  {lang}: {error}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Download Wikipedia dumps for all languages')
    parser.add_argument('--target-dir', default=DEFAULT_TARGET_DIR, 
                        help=f'Target directory for downloads (default: {DEFAULT_TARGET_DIR})')
    parser.add_argument('--languages', nargs='+', 
                        help='Specific languages to download (default: all)')
    parser.add_argument('--max-connections', type=int, default=MAX_CONNECTIONS,
                        help=f'Maximum concurrent connections (default: {MAX_CONNECTIONS})')
    parser.add_argument('--request-delay', type=int, default=2,
                        help='Delay in seconds between requests to avoid rate limiting (default: 2)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be downloaded without actually downloading')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        download_wikipedia_dumps(
            languages=args.languages,
            target_dir=args.target_dir,
            dry_run=args.dry_run,
            max_connections=args.max_connections,
            request_delay=args.request_delay
        )
    except KeyboardInterrupt:
        logger.info("\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
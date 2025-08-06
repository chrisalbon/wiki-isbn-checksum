# Lets go hunt some ISBNs!

import re
import csv
from datetime import datetime
import os
import argparse
import bz2
import xml.etree.ElementTree as ET
import glob
from multiprocessing import Pool, cpu_count
import sys

def get_language_from_dump_path(dump_path: str) -> str:
    """
    Extract language code from Wikipedia dump filename.
    
    Args:
        dump_path: Path to the dump file
        
    Returns:
        Language code (e.g., 'en', 'de', 'fr')
    """
    basename = os.path.basename(dump_path)
    # Pattern: langwiki-date-pages-articles...
    match = re.match(r'^([a-z]+)wiki-', basename)
    if match:
        return match.group(1)
    return 'en'  # Default to English if pattern doesn't match


def extract_articles_from_dump(dump_path: str):
    """
    Generator that yields (title, text) tuples from a Wikipedia dump file.
    
    Args:
        dump_path: Path to the .bz2 Wikipedia dump file
        
    Yields:
        Tuples of (title, text) for each article
    """
    # Define namespace - MediaWiki 0.11 format
    ns = {'mw': 'http://www.mediawiki.org/xml/export-0.11/'}
    
    with bz2.open(dump_path, 'rt', encoding='utf-8') as f:
        # Parse with namespace awareness
        context = ET.iterparse(f, events=('start', 'end'))
        context = iter(context)
        
        # Get the root element
        event, root = next(context)
        
        for event, elem in context:
            # Handle namespaced tags
            tag = elem.tag.split('}')[1] if '}' in elem.tag else elem.tag
            
            if event == 'end' and tag == 'page':
                # Extract page data
                title_elem = elem.find('.//mw:title', ns)
                text_elem = elem.find('.//mw:text', ns)
                ns_elem = elem.find('.//mw:ns', ns)
                redirect_elem = elem.find('.//mw:redirect', ns)
                
                # Skip if not in main namespace (0) or if redirect
                if ns_elem is not None and ns_elem.text == '0' and redirect_elem is None:
                    if title_elem is not None and text_elem is not None:
                        title = title_elem.text
                        text = text_elem.text or ""
                        
                        if title and text:
                            yield (title, text)
                
                # Clear the element to save memory
                elem.clear()
                # Also clear the root element's children
                root.clear()

def has_isbn_nearby(text: str, match_start: int, match_end: int, max_distance: int = 6) -> bool:
    """
    Check if 'ISBN' appears within max_distance characters before the match.
    
    Args:
        text: The full text
        match_start: Start position of the ISBN candidate
        match_end: End position of the ISBN candidate
        max_distance: Maximum characters to look before the match
        
    Returns:
        True if 'ISBN' is found within max_distance characters
    """
    # Look for ISBN within max_distance characters before the match
    search_start = max(0, match_start - max_distance - 4)  # -4 for 'ISBN' length
    search_text = text[search_start:match_start].lower()
    
    # Check if 'isbn' appears and is close enough
    isbn_pos = search_text.rfind('isbn')
    if isbn_pos != -1:
        # Calculate actual distance from 'isbn' end to match start
        actual_distance = len(search_text) - isbn_pos - 4
        return actual_distance <= max_distance
    
    return False


def find_potential_isbns(text: str, context_chars: int = 50, proximity: int = 6) -> list[dict]:
    """
    Finds all potential ISBN numbers in text with surrounding context.
    Only returns numbers that have 'ISBN' nearby in the context.

    Args:
        text: The text to search for ISBNs
        context_chars: Number of characters to include before and after ISBN

    Returns:
        List of dicts with 'isbn' and 'context' keys
    """
    # First, remove URLs from the text to avoid false positives
    url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
    text_without_urls = re.sub(url_pattern, ' ', text)
    
    # Look for sequences of digits with optional hyphens/spaces
    # Using negative lookbehind to ensure we don't start matching in the middle of a number
    pattern = r'(?<![0-9])(\d[\d\-\s]{8,16}[\dXx])\b'
    
    potential_isbns = []
    
    for match in re.finditer(pattern, text_without_urls):
        isbn = match.group(1)
        cleaned = re.sub(r'[-\s]', '', isbn).upper()
        
        # Only accept 10 or 13 character results
        if len(cleaned) == 10:
            if cleaned[:9].isdigit() and (cleaned[9].isdigit() or cleaned[9] == 'X'):
                # Get surrounding context
                start = max(0, match.start() - context_chars)
                end = min(len(text_without_urls), match.end() + context_chars)
                context = text_without_urls[start:end].strip()
                
                # Check if 'ISBN' appears within proximity characters before the match
                if has_isbn_nearby(text_without_urls, match.start(), match.end(), proximity):
                    potential_isbns.append({
                        'isbn': isbn,
                        'context': context
                    })
        elif len(cleaned) == 13:
            if cleaned.isdigit():
                # Get surrounding context
                start = max(0, match.start() - context_chars)
                end = min(len(text_without_urls), match.end() + context_chars)
                context = text_without_urls[start:end].strip()
                
                # Check if 'ISBN' appears within proximity characters before the match
                if has_isbn_nearby(text_without_urls, match.start(), match.end(), proximity):
                    potential_isbns.append({
                        'isbn': isbn,
                        'context': context
                    })
    
    return potential_isbns


def validate_isbn10(isbn: str) -> bool:
    """
    Validates an ISBN-10 using the check digit algorithm.
    
    Args:
        isbn: ISBN-10 string (hyphens/spaces will be removed)
        
    Returns:
        True if valid ISBN-10, False otherwise
    """
    # Remove hyphens and spaces
    cleaned = re.sub(r'[-\s]', '', isbn).upper()
    
    # Must be exactly 10 characters
    if len(cleaned) != 10:
        return False
    
    # First 9 must be digits, 10th can be digit or X
    if not cleaned[:9].isdigit():
        return False
    if not (cleaned[9].isdigit() or cleaned[9] == 'X'):
        return False
    
    # Calculate checksum
    total = 0
    for i in range(9):
        total += int(cleaned[i]) * (10 - i)
    
    # Add check digit
    if cleaned[9] == 'X':
        total += 10
    else:
        total += int(cleaned[9])
    
    return total % 11 == 0


def validate_isbn13(isbn: str) -> bool:
    """
    Validates an ISBN-13 using the check digit algorithm.
    
    Args:
        isbn: ISBN-13 string (hyphens/spaces will be removed)
        
    Returns:
        True if valid ISBN-13, False otherwise
    """
    # Remove hyphens and spaces
    cleaned = re.sub(r'[-\s]', '', isbn)
    
    # Must be exactly 13 digits
    if len(cleaned) != 13 or not cleaned.isdigit():
        return False
    
    # Calculate checksum
    total = 0
    for i in range(12):
        if i % 2 == 0:
            total += int(cleaned[i])
        else:
            total += int(cleaned[i]) * 3
    
    # Check digit calculation
    check_digit = (10 - (total % 10)) % 10
    
    return int(cleaned[12]) == check_digit


def deduplicate_isbns(isbns: list[str]) -> list[str]:
    """
    Removes duplicate ISBNs by normalizing them. Normalizing catches
    duplicates that are formatted differently 
    
    Args:
        isbns: List of ISBN strings (may contain hyphens/spaces)
        
    Returns:
        List of unique ISBNs in their original format
    """
    seen_normalized = set()
    unique_isbns = []
    
    for isbn in isbns:
        # Normalize by removing hyphens and spaces
        normalized = re.sub(r'[-\s]', '', isbn).upper()
        
        if normalized not in seen_normalized:
            seen_normalized.add(normalized)
            unique_isbns.append(isbn)
    
    return unique_isbns


def process_single_dump_worker(args: tuple) -> tuple[str, list[dict], float, str, int]:
    """
    Worker function for multiprocessing that wraps process_single_dump.
    
    Args:
        args: Tuple of (dump_path, context_chars, proximity)
        
    Returns:
        Tuple of (dump_path, results, elapsed_time, error_message, article_count)
    """
    dump_path, context_chars, proximity = args
    try:
        # Run in quiet mode for parallel processing
        results, elapsed, article_count = process_single_dump(dump_path, context_chars, proximity, quiet=True)
        
        # Print completion message for this dump
        language = get_language_from_dump_path(dump_path)
        print(f"[{language.upper()}] Completed: {os.path.basename(dump_path)} - Processed {article_count} articles, found ISBNs in {len(results)} ({elapsed:.1f}s)")
        
        return (dump_path, results, elapsed, None, article_count)
    except Exception as e:
        print(f"Error processing {dump_path}: {str(e)}", file=sys.stderr)
        return (dump_path, [], 0.0, str(e), 0)


def process_single_dump(dump_path: str, context_chars: int = 50, proximity: int = 6, quiet: bool = False) -> tuple[list[dict], float, int]:
    """
    Process a single Wikipedia dump file to extract and validate ISBNs.
    
    Args:
        dump_path: Path to the dump file
        context_chars: Number of context characters around ISBN
        quiet: If True, suppress progress output (for parallel processing)
        
    Returns:
        Tuple of (results list, processing time in seconds, total articles processed)
    """
    # Get language from dump filename
    language = get_language_from_dump_path(dump_path)
    
    results = []
    article_count = 0
    start_time = datetime.now()
    
    if not quiet:
        print(f"\nProcessing dump: {os.path.basename(dump_path)} (Language: {language})")
        print("="*60)
    
    for title, text in extract_articles_from_dump(dump_path):
        article_count += 1
        
        # Find ISBNs in article text
        isbn_results = find_potential_isbns(text, context_chars, proximity)
        
        if isbn_results:  # Only process articles with ISBNs
            valid_isbns = []
            invalid_isbns = []
            
            for result in isbn_results:
                isbn = result['isbn']
                cleaned = re.sub(r'[-\s]', '', isbn)
                
                # Check validity
                if len(cleaned) == 10:
                    is_valid = validate_isbn10(isbn)
                elif len(cleaned) == 13:
                    is_valid = validate_isbn13(isbn)
                else:
                    is_valid = False
                
                if is_valid:
                    valid_isbns.append(result)
                else:
                    invalid_isbns.append(result)
            
            article_result = {
                'title': title,
                'language': language,
                'url': f"https://{language}.wikipedia.org/wiki/{title.replace(' ', '_')}",
                'total_found': len(isbn_results),
                'valid_isbns': valid_isbns,
                'invalid_isbns': invalid_isbns
            }
            
            results.append(article_result)
            
            # Progress update every 100 articles with ISBNs
            if not quiet and len(results) % 100 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = article_count / elapsed if elapsed > 0 else 0
                print(f"  [{language.upper()}] Processed {article_count} articles, found ISBNs in {len(results)} ({rate:.1f} articles/sec)")
    
    # Final stats for this dump
    elapsed = (datetime.now() - start_time).total_seconds()
    if not quiet:
        print(f"  [{language.upper()}] Completed: {article_count} articles processed in {elapsed:.1f}s")
        print(f"  [{language.upper()}] Found ISBNs in {len(results)} articles")
    
    return results, elapsed, article_count


def process_all_dumps(dumps_dir: str = "./dumps", context_chars: int = 50, proximity: int = 6, workers: int = 1) -> tuple[list[dict], list[str], datetime, datetime, dict[str, float], int, dict[str, int]]:
    """
    Process all Wikipedia dump files in a directory.
    
    Args:
        dumps_dir: Directory containing dump files
        context_chars: Number of context characters around ISBN
        proximity: Maximum characters between ISBN and number
        workers: Number of parallel workers (default 1 for sequential, -1 for all CPUs)
        
    Returns:
        Tuple of (results, dump_files, start_time, end_time, language_times, total_articles_processed, language_article_counts)
    """
    all_results = []
    language_times = {}  # Track processing time per language
    total_articles_processed = 0  # Track total articles across all dumps
    language_article_counts = {}  # Track total articles per language
    
    # Find all .bz2 files in dumps directory
    dump_files = sorted(glob.glob(os.path.join(dumps_dir, "*.bz2")))
    
    if not dump_files:
        print(f"No .bz2 dump files found in {dumps_dir}")
        return [], [], datetime.now(), datetime.now(), {}, 0, {}
    
    print(f"Found {len(dump_files)} dump file(s) to process")
    
    # Determine number of workers
    if workers == -1:
        workers = cpu_count() - 1  # Leave one core free
    workers = max(1, min(workers, len(dump_files), cpu_count()))
    
    start_time = datetime.now()
    
    if workers == 1:
        # Sequential processing (original behavior)
        print("Processing dumps sequentially...")
        for dump_path in dump_files:
            # Get language from filename
            language = get_language_from_dump_path(dump_path)
            
            # Process dump and get results with timing
            dump_results, dump_time, article_count = process_single_dump(dump_path, context_chars, proximity)
            all_results.extend(dump_results)
            total_articles_processed += article_count
            
            # Accumulate time for this language
            if language not in language_times:
                language_times[language] = 0.0
            language_times[language] += dump_time
            
            # Track articles per language
            if language not in language_article_counts:
                language_article_counts[language] = 0
            language_article_counts[language] += article_count
    else:
        # Parallel processing
        print(f"Processing dumps in parallel with {workers} workers...")
        
        # Prepare arguments for worker function
        worker_args = [(dump_path, context_chars, proximity) for dump_path in dump_files]
        
        # Process dumps in parallel
        with Pool(workers) as pool:
            # Use imap_unordered for better progress tracking
            results = pool.map(process_single_dump_worker, worker_args)
        
        # Process results
        for dump_path, dump_results, dump_time, error, article_count in results:
            if error:
                print(f"Failed to process {dump_path}: {error}")
                continue
                
            language = get_language_from_dump_path(dump_path)
            all_results.extend(dump_results)
            total_articles_processed += article_count
            
            # Accumulate time for this language
            if language not in language_times:
                language_times[language] = 0.0
            language_times[language] += dump_time
            
            # Track articles per language
            if language not in language_article_counts:
                language_article_counts[language] = 0
            language_article_counts[language] += article_count
    
    end_time = datetime.now()
    
    return all_results, dump_files, start_time, end_time, language_times, total_articles_processed, language_article_counts


def save_report(results: list[dict], dump_files: list[str], start_time: datetime, end_time: datetime, language_times: dict[str, float] = None, total_articles_processed: int = None, language_article_counts: dict[str, int] = None, filename: str = None) -> str:
    """
    Save a detailed report of the ISBN extraction run.
    
    Args:
        results: List of article results
        dump_files: List of dump files processed
        start_time: When processing started
        end_time: When processing ended
        language_times: Dictionary of processing times per language
        filename: Optional filename, defaults to timestamp.txt
        
    Returns:
        Path to the created report file
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}.txt"
    
    # Create data directory if it doesn't exist
    os.makedirs("../data", exist_ok=True)
    filepath = os.path.join("../data", filename)
    
    # Calculate statistics
    articles_with_isbns = len(results)
    if total_articles_processed is None:
        total_articles_processed = articles_with_isbns  # Fallback for backwards compatibility
    total_valid = sum(len(r['valid_isbns']) for r in results)
    total_invalid = sum(len(r['invalid_isbns']) for r in results)
    total_isbns = total_valid + total_invalid
    processing_time = (end_time - start_time).total_seconds()
    
    # Count unique ISBNs and languages
    unique_valid = set()
    unique_invalid = set()
    languages = {}
    for r in results:
        lang = r.get('language', 'en')
        if lang not in languages:
            languages[lang] = {
                'articles': 0,
                'valid_isbns': 0,
                'invalid_isbns': 0,
                'unique_valid': set(),
                'unique_invalid': set()
            }
        languages[lang]['articles'] += 1
        languages[lang]['valid_isbns'] += len(r['valid_isbns'])
        languages[lang]['invalid_isbns'] += len(r['invalid_isbns'])
        
        for isbn in r['valid_isbns']:
            normalized = re.sub(r'[-\s]', '', isbn['isbn'])
            unique_valid.add(normalized)
            languages[lang]['unique_valid'].add(normalized)
        for isbn in r['invalid_isbns']:
            normalized = re.sub(r'[-\s]', '', isbn['isbn'])
            unique_invalid.add(normalized)
            languages[lang]['unique_invalid'].add(normalized)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("Wikipedia ISBN Extraction Report\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Processing Time: {processing_time:.1f} seconds\n")
        f.write(f"Processing Speed: {total_articles_processed/processing_time:.1f} articles/second\n\n")
        
        f.write(f"Wikis Processed: {len(dump_files)}\n")
        f.write("Dump Files:\n")
        for dump_file in dump_files:
            f.write(f"  - {os.path.basename(dump_file)}\n")
        f.write("\n")
        
        f.write("Article Statistics:\n")
        f.write(f"  Total articles processed: {total_articles_processed:,}\n")
        f.write(f"  Articles with ISBNs: {articles_with_isbns:,}\n")
        f.write(f"  Articles without ISBNs: {total_articles_processed - articles_with_isbns:,}\n\n")
        
        f.write("ISBN Statistics:\n")
        f.write(f"  Total ISBNs found: {total_isbns:,}\n")
        f.write(f"  Valid ISBNs (checksum passed): {total_valid:,}\n")
        f.write(f"  Invalid ISBNs (checksum failed): {total_invalid:,}\n")
        f.write(f"  Pass rate: {(total_valid/total_isbns*100) if total_isbns > 0 else 0:.2f}%\n\n")
        
        f.write("Unique ISBN Statistics:\n")
        f.write(f"  Unique valid ISBNs: {len(unique_valid):,}\n")
        f.write(f"  Unique invalid ISBNs: {len(unique_invalid):,}\n\n")
        
        f.write("Format Breakdown:\n")
        isbn10_valid = sum(1 for r in results for isbn in r['valid_isbns'] 
                          if len(re.sub(r'[-\s]', '', isbn['isbn'])) == 10)
        isbn13_valid = sum(1 for r in results for isbn in r['valid_isbns'] 
                          if len(re.sub(r'[-\s]', '', isbn['isbn'])) == 13)
        isbn10_invalid = sum(1 for r in results for isbn in r['invalid_isbns'] 
                            if len(re.sub(r'[-\s]', '', isbn['isbn'])) == 10)
        isbn13_invalid = sum(1 for r in results for isbn in r['invalid_isbns'] 
                            if len(re.sub(r'[-\s]', '', isbn['isbn'])) == 13)
        
        f.write(f"  ISBN-10 (valid): {isbn10_valid:,}\n")
        f.write(f"  ISBN-10 (invalid): {isbn10_invalid:,}\n")
        f.write(f"  ISBN-13 (valid): {isbn13_valid:,}\n")
        f.write(f"  ISBN-13 (invalid): {isbn13_invalid:,}\n\n")
        
        if len(languages) > 1:
            f.write("Language Breakdown:\n")
            for lang in sorted(languages.keys()):
                lang_data = languages[lang]
                lang_total = lang_data['valid_isbns'] + lang_data['invalid_isbns']
                lang_pass_rate = (lang_data['valid_isbns']/lang_total*100) if lang_total > 0 else 0
                f.write(f"\n  {lang.upper()}:\n")
                if language_article_counts and lang in language_article_counts:
                    f.write(f"    Total articles processed: {language_article_counts[lang]:,}\n")
                f.write(f"    Articles with ISBNs: {lang_data['articles']:,}\n")
                f.write(f"    Total ISBNs: {lang_total:,}\n")
                f.write(f"    Valid ISBNs: {lang_data['valid_isbns']:,}\n")
                f.write(f"    Invalid ISBNs: {lang_data['invalid_isbns']:,}\n")
                f.write(f"    Pass rate: {lang_pass_rate:.2f}%\n")
                f.write(f"    Unique valid: {len(lang_data['unique_valid']):,}\n")
                f.write(f"    Unique invalid: {len(lang_data['unique_invalid']):,}\n")
                if language_times and lang in language_times:
                    f.write(f"    Processing time: {language_times[lang]:.1f}s\n")
                    if language_article_counts and lang in language_article_counts:
                        f.write(f"    Speed: {language_article_counts[lang]/language_times[lang]:.1f} articles/sec\n")
    
    return filepath


def save_failed_isbns_to_csv(results: list[dict], filename: str = None) -> str:
    """
    Save only failed ISBNs to a CSV file for inspection.
    
    Args:
        results: List of article results
        filename: Optional filename, defaults to timestamp-based name
        
    Returns:
        Path to the created CSV file
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}.csv"
    
    # Create data directory if it doesn't exist
    os.makedirs("../data", exist_ok=True)
    filepath = os.path.join("../data", filename)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['article_title', 'language', 'isbn', 'format', 'context', 'article_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        # Only write invalid ISBNs
        for result in results:
            article_title = result['title']
            article_url = result.get('url', '')
            
            for isbn_data in result.get('invalid_isbns', []):
                isbn = isbn_data['isbn']
                cleaned = re.sub(r'[-\s]', '', isbn)
                
                # Determine ISBN format
                if len(cleaned) == 10:
                    isbn_format = 'ISBN-10'
                elif len(cleaned) == 13:
                    isbn_format = 'ISBN-13'
                else:
                    isbn_format = f'Invalid ({len(cleaned)} digits)'
                
                writer.writerow({
                    'article_title': article_title,
                    'language': result.get('language', 'en'),
                    'isbn': isbn,
                    'format': isbn_format,
                    'context': isbn_data['context'],
                    'article_url': article_url
                })
    
    return filepath


def main():
    parser = argparse.ArgumentParser(description='Extract ISBNs from Wikipedia dump files')
    parser.add_argument('--dumps-dir', default='../dumps', help='Directory containing Wikipedia dump files (default: ../dumps)')
    parser.add_argument('--context', type=int, default=50, help='Number of context characters around ISBN (default: 50)')
    parser.add_argument('--proximity', type=int, default=6, help='Maximum characters between ISBN and number (default: 6)')
    parser.add_argument('--workers', type=int, default=1, help='Number of parallel workers (-1 for all CPUs, default: 1)')
    parser.add_argument('--output-prefix', help='Output file prefix (default: timestamp)')
    
    args = parser.parse_args()
    
    # Process all dump files
    results, dump_files, start_time, end_time, language_times, total_articles_processed, language_article_counts = process_all_dumps(
        args.dumps_dir, args.context, args.proximity, args.workers
    )
    
    if not results:
        print("No articles found to process.")
        return
    
    # Print summary
    print(f"\n{'='*60}")
    print("OVERALL SUMMARY")
    print('='*60)
    
    total_valid = sum(len(r['valid_isbns']) for r in results)
    total_invalid = sum(len(r['invalid_isbns']) for r in results)
    articles_with_isbns = len(results)
    total_time = (end_time - start_time).total_seconds()
    
    print(f"Total articles processed: {total_articles_processed:,}")
    print(f"Articles with ISBNs: {articles_with_isbns:,}")
    print(f"Total valid ISBNs found: {total_valid:,}")
    print(f"Total invalid ISBNs found: {total_invalid:,}")
    print(f"Total processing time: {total_time:.1f}s")
    
    # Print time breakdown by language
    if language_times:
        print(f"\nProcessing time by language:")
        for lang in sorted(language_times.keys()):
            lang_time = language_times[lang]
            lang_articles_with_isbns = len([r for r in results if r.get('language', 'en') == lang])
            lang_total_articles = language_article_counts.get(lang, lang_articles_with_isbns) if language_article_counts else lang_articles_with_isbns
            speed = lang_total_articles / lang_time if lang_time > 0 else 0
            print(f"  {lang.upper()}: {lang_time:.1f}s ({lang_total_articles:,} articles processed, {lang_articles_with_isbns:,} with ISBNs, {speed:.1f} articles/sec)")
    
    # Generate timestamp for both files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save report
    if args.output_prefix:
        report_filename = f"{args.output_prefix}.txt"
        csv_filename = f"{args.output_prefix}.csv"
    else:
        report_filename = f"{timestamp}.txt"
        csv_filename = f"{timestamp}.csv"
    
    report_path = save_report(results, dump_files, start_time, end_time, language_times, total_articles_processed, language_article_counts, report_filename)
    print(f"\nDetailed report saved to: {report_path}")
    
    # Save failed ISBNs to CSV
    if total_invalid > 0:
        csv_path = save_failed_isbns_to_csv(results, csv_filename)
        print(f"Failed ISBNs saved to: {csv_path}")
    else:
        print("No failed ISBNs found - CSV not created")

if __name__ == "__main__":
    main()
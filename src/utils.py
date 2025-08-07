#!/usr/bin/env python3
"""
Utility functions for Wikipedia ISBN analysis.

This module contains helper functions for analyzing ISBN validation results,
checking dump file formats, and extracting statistics from reports.
"""

import re
import bz2
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Tuple, Optional


def check_dump_namespace(dump_path: str) -> Optional[str]:
    """
    Check the XML namespace used in a Wikipedia dump file.
    
    Args:
        dump_path: Path to the Wikipedia dump file
        
    Returns:
        The namespace URI if found, None otherwise
    """
    try:
        with bz2.open(dump_path, 'rt', encoding='utf-8') as f:
            # Read first few KB to find namespace
            content = f.read(4096)
            
            if 'xmlns=' in content:
                # Extract namespace
                start = content.find('xmlns="') + 7
                end = content.find('"', start)
                return content[start:end]
    except Exception as e:
        print(f"Error checking namespace: {e}")
    
    return None


def extract_pass_rates_from_report(file_path: str) -> List[Tuple[str, float]]:
    """
    Extract ISBN pass rates for each wiki from a report file.
    
    Args:
        file_path: Path to the report text file
        
    Returns:
        List of tuples containing (wiki_code, pass_rate)
    """
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find the language breakdown section
    language_section_start = content.find("Language Breakdown:")
    if language_section_start == -1:
        return []
    
    # Extract content after Language Breakdown
    language_content = content[language_section_start:]
    
    # Pattern to match wiki entries with their statistics
    wiki_pattern = r'  ([A-Z]+(?:-[A-Z]+)?):\n.*?Pass rate: ([\d.]+)%'
    
    matches = re.findall(wiki_pattern, language_content, re.DOTALL)
    
    # Create list of tuples (wiki_code, pass_rate)
    results = []
    for wiki_code, pass_rate in matches:
        results.append((wiki_code, float(pass_rate)))
    
    return results


def analyze_invalid_isbns(csv_path: str) -> Dict[str, any]:
    """
    Analyze patterns in invalid ISBNs from a CSV file.
    
    Args:
        csv_path: Path to the CSV file containing invalid ISBNs
        
    Returns:
        Dictionary containing analysis results
    """
    import csv
    from collections import Counter, defaultdict
    
    stats = {
        'total_invalid': 0,
        'by_format': Counter(),
        'by_language': Counter(),
        'common_errors': [],
        'articles_with_most_errors': []
    }
    
    article_errors = defaultdict(int)
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                stats['total_invalid'] += 1
                stats['by_format'][row.get('format', 'Unknown')] += 1
                stats['by_language'][row.get('language', 'unknown')] += 1
                article_errors[row.get('article_title', 'Unknown')] += 1
        
        # Get top articles with most errors
        stats['articles_with_most_errors'] = article_errors.most_common(10)
        
    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
    except Exception as e:
        print(f"Error analyzing CSV: {e}")
    
    return stats


def compare_dump_structures(dump1_path: str, dump2_path: str) -> Dict[str, any]:
    """
    Compare the structure of two Wikipedia dump files.
    
    Args:
        dump1_path: Path to first dump file
        dump2_path: Path to second dump file
        
    Returns:
        Dictionary containing comparison results
    """
    def get_elements(dump_path: str, max_pages: int = 5) -> set:
        """Extract element names from a dump file."""
        elements = set()
        
        try:
            with bz2.open(dump_path, 'rt', encoding='utf-8') as f:
                context = ET.iterparse(f, events=('start', 'end'))
                context = iter(context)
                
                # Get root
                event, root = next(context)
                
                page_count = 0
                for event, elem in context:
                    tag = elem.tag.split('}')[1] if '}' in elem.tag else elem.tag
                    elements.add(tag)
                    
                    if event == 'end' and tag == 'page':
                        page_count += 1
                        if page_count >= max_pages:
                            break
                    
                    elem.clear()
                    root.clear()
        except Exception as e:
            print(f"Error processing {dump_path}: {e}")
        
        return elements
    
    elements1 = get_elements(dump1_path)
    elements2 = get_elements(dump2_path)
    
    return {
        'dump1_namespace': check_dump_namespace(dump1_path),
        'dump2_namespace': check_dump_namespace(dump2_path),
        'common_elements': elements1 & elements2,
        'only_in_dump1': elements1 - elements2,
        'only_in_dump2': elements2 - elements1
    }


def format_statistics_summary(results: List[dict]) -> str:
    """
    Format a summary of ISBN validation statistics.
    
    Args:
        results: List of article results from main.py
        
    Returns:
        Formatted string with statistics
    """
    total_valid = sum(len(r['valid_isbns']) for r in results)
    total_invalid = sum(len(r['invalid_isbns']) for r in results)
    total_isbns = total_valid + total_invalid
    
    if total_isbns == 0:
        return "No ISBNs found"
    
    pass_rate = (total_valid / total_isbns) * 100
    
    # Count unique ISBNs
    unique_valid = set()
    unique_invalid = set()
    
    for r in results:
        for isbn in r['valid_isbns']:
            normalized = re.sub(r'[-\s]', '', isbn['isbn'])
            unique_valid.add(normalized)
        for isbn in r['invalid_isbns']:
            normalized = re.sub(r'[-\s]', '', isbn['isbn'])
            unique_invalid.add(normalized)
    
    return f"""
ISBN Validation Summary
=======================
Total ISBNs found: {total_isbns:,}
Valid ISBNs: {total_valid:,} ({pass_rate:.2f}%)
Invalid ISBNs: {total_invalid:,} ({100-pass_rate:.2f}%)

Unique ISBNs:
- Valid: {len(unique_valid):,}
- Invalid: {len(unique_invalid):,}

Articles processed: {len(results):,}
"""


def estimate_processing_time(file_size_bytes: int, articles_per_second: float = 500) -> str:
    """
    Estimate processing time for a dump file based on its size.
    
    Args:
        file_size_bytes: Size of the dump file in bytes
        articles_per_second: Expected processing speed
        
    Returns:
        Human-readable time estimate
    """
    # Rough estimate: ~2000 bytes per article on average
    estimated_articles = file_size_bytes / 2000
    estimated_seconds = estimated_articles / articles_per_second
    
    hours = int(estimated_seconds // 3600)
    minutes = int((estimated_seconds % 3600) // 60)
    seconds = int(estimated_seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


if __name__ == "__main__":
    # Example usage
    print("Wikipedia ISBN Utils Module")
    print("This module provides utility functions for ISBN analysis.")
    print("\nAvailable functions:")
    print("- check_dump_namespace(): Check XML namespace version")
    print("- extract_pass_rates_from_report(): Extract pass rates from reports")
    print("- analyze_invalid_isbns(): Analyze patterns in invalid ISBNs")
    print("- compare_dump_structures(): Compare two dump file structures")
    print("- format_statistics_summary(): Format statistics summary")
    print("- estimate_processing_time(): Estimate processing time")
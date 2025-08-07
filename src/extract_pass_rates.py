#!/usr/bin/env python3
"""Extract ISBN pass rates from the report file."""

import re
from pathlib import Path

def extract_pass_rates(file_path):
    """Extract pass rates for each wiki from the report file."""
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find the language breakdown section
    language_section_start = content.find("Language Breakdown:")
    if language_section_start == -1:
        print("Language Breakdown section not found")
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

def main():
    file_path = Path("/Users/chrisalbon/Documents/second_brain/projects/archive/wiki_isbn_checksum/data/20250806_180355.txt")
    
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return
    
    pass_rates = extract_pass_rates(file_path)
    
    # Sort by pass rate (ascending to see worst performers first)
    pass_rates.sort(key=lambda x: x[1])
    
    print("ISBN Pass Rates by Wiki (sorted by pass rate):")
    print("=" * 50)
    print(f"{'Wiki':<15} {'Pass Rate':>10}")
    print("-" * 50)
    
    for wiki, rate in pass_rates:
        print(f"{wiki:<15} {rate:>9.2f}%")
    
    print("-" * 50)
    print(f"Total wikis: {len(pass_rates)}")
    
    if pass_rates:
        avg_rate = sum(r for _, r in pass_rates) / len(pass_rates)
        print(f"Average pass rate: {avg_rate:.2f}%")
        print(f"Minimum pass rate: {pass_rates[0][0]} - {pass_rates[0][1]:.2f}%")
        print(f"Maximum pass rate: {pass_rates[-1][0]} - {pass_rates[-1][1]:.2f}%")

if __name__ == "__main__":
    main()
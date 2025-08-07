#!/usr/bin/env python3
"""
Analyze invalid ISBNs from Wikipedia extraction to identify patterns and common issues.
"""

import pandas as pd
import re
from collections import Counter, defaultdict
from datetime import datetime
import os

def normalize_isbn(isbn):
    """Remove hyphens and spaces, convert to uppercase."""
    if pd.isna(isbn):
        return ""
    return re.sub(r'[-\s]', '', str(isbn)).upper()

def is_isbn_like(isbn):
    """Check if string looks like an ISBN (digits and possibly X at end)."""
    if not isbn:
        return False
    return bool(re.match(r'^[\dX]+$', isbn)) and len(isbn) in [10, 13]

def classify_isbn_error(isbn):
    """Classify the type of ISBN error."""
    normalized = normalize_isbn(isbn)
    
    if not normalized:
        return "Empty"
    
    if not is_isbn_like(normalized):
        return "Invalid Format"
    
    # Check for placeholder patterns
    if re.match(r'^0+[0-9X]?$', normalized):
        return "All Zeros Placeholder"
    if re.match(r'^123456789[0-9X]?$', normalized):
        return "Sequential Placeholder"
    if re.match(r'^(\d)\1+[0-9X]?$', normalized):
        return "Repeated Digit Placeholder"
    
    # Check length
    if len(normalized) == 10:
        return "ISBN-10 Checksum Failed"
    elif len(normalized) == 13:
        return "ISBN-13 Checksum Failed"
    else:
        return "Wrong Length"

def get_isbn_prefix(isbn):
    """Extract country/publisher prefix from ISBN."""
    normalized = normalize_isbn(isbn)
    if not is_isbn_like(normalized):
        return None
    
    if len(normalized) == 13:
        # For ISBN-13, get the registration group (country/language)
        if normalized.startswith('978'):
            # Legacy ISBN-10 compatible
            prefix = normalized[3:6]  # Get first 3 digits after 978
        elif normalized.startswith('979'):
            # New ISBN-13 only
            prefix = normalized[3:6]
        else:
            return normalized[:3]
    elif len(normalized) == 10:
        # For ISBN-10, get the registration group
        prefix = normalized[:3]
    else:
        return None
    
    return prefix

def analyze_invalid_isbns(csv_path):
    """Main analysis function."""
    print("Loading data...")
    
    # Load CSV with proper handling of embedded commas
    df = pd.read_csv(csv_path, encoding='utf-8', on_bad_lines='skip')
    
    print(f"Loaded {len(df)} invalid ISBN records")
    
    # Clean and normalize ISBNs
    df['isbn_normalized'] = df['isbn'].apply(normalize_isbn)
    df['is_isbn_like'] = df['isbn_normalized'].apply(is_isbn_like)
    
    # Filter to actual ISBN-like entries for some analyses
    df_isbns = df[df['is_isbn_like']].copy()
    
    print(f"Found {len(df_isbns)} ISBN-like entries")
    print(f"Found {len(df) - len(df_isbns)} non-ISBN entries (parsing errors, etc.)")
    
    results = {}
    
    # 1. Most common invalid ISBNs
    print("\nAnalyzing most common invalid ISBNs...")
    isbn_counts = df_isbns['isbn_normalized'].value_counts()
    results['most_common'] = isbn_counts.head(50)
    
    # 2. Cross-wiki analysis
    print("Analyzing cross-wiki occurrences...")
    isbn_languages = defaultdict(set)
    for _, row in df_isbns.iterrows():
        if pd.notna(row['language']):
            isbn_languages[row['isbn_normalized']].add(row['language'])
    
    multi_wiki_isbns = {isbn: langs for isbn, langs in isbn_languages.items() 
                        if len(langs) > 1}
    results['multi_wiki'] = sorted(multi_wiki_isbns.items(), 
                                   key=lambda x: len(x[1]), reverse=True)[:20]
    
    # 3. Language distribution
    print("Analyzing language distribution...")
    language_counts = df['language'].value_counts()
    results['languages'] = language_counts.head(20)
    
    # 4. Format distribution
    print("Analyzing format distribution...")
    format_counts = df['format'].value_counts()
    results['formats'] = format_counts
    
    # 5. Error classification
    print("Classifying error types...")
    df_isbns['error_type'] = df_isbns['isbn_normalized'].apply(classify_isbn_error)
    error_counts = df_isbns['error_type'].value_counts()
    results['error_types'] = error_counts
    
    # 6. ISBN prefix analysis (for ISBN-like entries)
    print("Analyzing ISBN prefixes...")
    df_isbns['prefix'] = df_isbns['isbn_normalized'].apply(get_isbn_prefix)
    prefix_counts = df_isbns['prefix'].dropna().value_counts()
    results['prefixes'] = prefix_counts.head(20)
    
    # 7. Most common ISBNs per language
    print("Analyzing per-language patterns...")
    lang_top_isbns = {}
    for lang in language_counts.head(10).index:
        if pd.notna(lang) and lang not in ['', '}}', '|-']:  # Skip non-language entries
            lang_df = df_isbns[df_isbns['language'] == lang]
            if len(lang_df) > 0:
                lang_top = lang_df['isbn_normalized'].value_counts().head(5)
                lang_top_isbns[lang] = lang_top
    results['lang_top_isbns'] = lang_top_isbns
    
    # 8. Statistics
    print("Calculating statistics...")
    total_instances = len(df)
    isbn_like_instances = len(df_isbns)
    unique_isbns = df_isbns['isbn_normalized'].nunique()
    duplication_rate = isbn_like_instances / unique_isbns if unique_isbns > 0 else 0
    
    # Pareto analysis
    cumsum = isbn_counts.cumsum()
    total_isbn_instances = isbn_counts.sum()
    pareto_80 = len(cumsum[cumsum <= total_isbn_instances * 0.8])
    pareto_percent = (pareto_80 / len(isbn_counts) * 100) if len(isbn_counts) > 0 else 0
    
    results['statistics'] = {
        'total_instances': total_instances,
        'isbn_like_instances': isbn_like_instances,
        'unique_isbns': unique_isbns,
        'duplication_rate': duplication_rate,
        'pareto_isbns': pareto_80,
        'pareto_percent': pareto_percent,
        'total_unique_in_pareto': len(isbn_counts)
    }
    
    # 9. Check for specific problematic ISBNs
    print("Identifying problematic patterns...")
    problematic = []
    
    # ISBNs appearing more than 100 times
    very_common = isbn_counts[isbn_counts > 100]
    for isbn, count in very_common.items():
        langs = list(isbn_languages.get(isbn, set()))
        problematic.append({
            'isbn': isbn,
            'count': count,
            'languages': langs,
            'type': classify_isbn_error(isbn)
        })
    
    results['problematic'] = problematic
    
    return results

def generate_report(results, output_path):
    """Generate a comprehensive text report."""
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("WIKIPEDIA INVALID ISBN ANALYSIS REPORT\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Executive Summary
        stats = results['statistics']
        f.write("EXECUTIVE SUMMARY\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total Invalid ISBNs Found: {stats['total_instances']:,}\n")
        f.write(f"Properly Formatted (but invalid): {stats['isbn_like_instances']:,}\n")
        f.write(f"Unique Invalid ISBNs: {stats['unique_isbns']:,}\n")
        f.write(f"Average Occurrences per ISBN: {stats['duplication_rate']:.2f}x\n")
        f.write(f"Concentration: {stats['pareto_percent']:.1f}% of unique ISBNs cause 80% of all errors\n")
        f.write(f"              ({stats['pareto_isbns']:,} ISBNs out of {stats['total_unique_in_pareto']:,} total unique)\n\n")
        
        # Most Common Invalid ISBNs
        f.write("\n" + "=" * 80 + "\n")
        f.write("MOST COMMON INVALID ISBNs (Top 30)\n")
        f.write("-" * 40 + "\n")
        f.write(f"{'Rank':<6} {'ISBN':<20} {'Count':<10} {'% of Total':<12}\n")
        f.write("-" * 60 + "\n")
        
        # Use total ISBN-like instances for percentage calculation
        total_isbn_instances = stats['isbn_like_instances']
        for i, (isbn, count) in enumerate(results['most_common'].head(30).items(), 1):
            percentage = (count / total_isbn_instances * 100) if total_isbn_instances > 0 else 0
            f.write(f"{i:<6} {isbn:<20} {count:<10,} {percentage:>11.2f}%\n")
        
        # Error Type Classification
        f.write("\n" + "=" * 80 + "\n")
        f.write("ERROR TYPE CLASSIFICATION\n")
        f.write("-" * 40 + "\n")
        
        total_classified = results['error_types'].sum()
        f.write(f"{'Error Type':<30} {'Count':<12} {'Percentage':<12}\n")
        f.write("-" * 60 + "\n")
        for error_type, count in results['error_types'].items():
            percentage = (count / total_classified * 100) if total_classified > 0 else 0
            f.write(f"{error_type:<30} {count:<12,} {percentage:>11.2f}%\n")
        
        # Language Distribution
        f.write("\n" + "=" * 80 + "\n")
        f.write("LANGUAGE DISTRIBUTION (Top 20)\n")
        f.write("-" * 40 + "\n")
        
        total_entries = results['languages'].sum()
        f.write(f"{'Rank':<6} {'Language':<15} {'Count':<12} {'Percentage':<12}\n")
        f.write("-" * 50 + "\n")
        for i, (lang, count) in enumerate(results['languages'].items(), 1):
            if pd.notna(lang) and lang not in ['', '}}', '|-']:
                percentage = (count / total_entries * 100) if total_entries > 0 else 0
                f.write(f"{i:<6} {str(lang):<15} {count:<12,} {percentage:>11.2f}%\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 80 + "\n")

def main():
    # Paths
    csv_path = "../data/20250806_180355.csv"
    output_path = "../data/invalid_isbn_analysis.txt"
    
    # Check if CSV exists
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return
    
    print("Starting Invalid ISBN Analysis...")
    print("=" * 60)
    
    # Run analysis
    results = analyze_invalid_isbns(csv_path)
    
    # Generate report
    print("\nGenerating report...")
    generate_report(results, output_path)
    
    print(f"\nAnalysis complete! Report saved to: {output_path}")
    
    # Print quick summary
    stats = results['statistics']
    print("\nQuick Summary:")
    print(f"  Total instances analyzed: {stats['total_instances']:,}")
    print(f"  Unique invalid ISBNs: {stats['unique_isbns']:,}")
    print(f"  Average duplication: {stats['duplication_rate']:.2f}x")
    print(f"  Most common invalid ISBN: {results['most_common'].index[0]} ({results['most_common'].iloc[0]:,} times)")

if __name__ == "__main__":
    main()
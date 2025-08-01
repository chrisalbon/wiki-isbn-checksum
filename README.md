# Wikipedia ISBN Checksum Validator

A Python tool that processes Wikipedia XML dump files to extract and validate ISBN numbers found in articles. This tool was created to analyze the accuracy of ISBN checksums in Wikipedia content.

## Overview

This tool:
- Processes compressed Wikipedia XML dumps (.bz2 files)
- Extracts all ISBN numbers from article text
- Validates ISBN-10 and ISBN-13 checksums
- Generates detailed reports on validation results
- Exports invalid ISBNs with context for manual review

## Key Features

- **High Performance**: Processes ~330 articles/second
- **Memory Efficient**: Streams XML data to handle multi-GB dump files
- **Multi-Language Support**: Automatically detects and processes dumps from any Wikipedia language edition
- **Context-Aware**: Only extracts numbers that appear near "ISBN" text to reduce false positives
- **Comprehensive Validation**: Implements proper checksum algorithms for both ISBN-10 and ISBN-13
- **Detailed Reporting**: Provides statistics on total/unique ISBNs, pass rates, and format breakdown
- **Language Analysis**: Breaks down ISBN statistics by Wikipedia language when processing multiple languages
- **Deduplication**: Normalizes ISBNs to catch differently-formatted duplicates

## How It Works

1. **XML Processing**: Reads compressed Wikipedia dumps using streaming XML parsing
2. **ISBN Detection**: 
   - First removes URLs from text to avoid false positives
   - Uses regex pattern `(?<![0-9])(\d[\d\-\s]{8,16}[\dXx])\b` to find sequences of digits (with optional hyphens/spaces) that could be ISBNs
   - Pattern uses negative lookbehind to capture complete ISBN-13s (e.g., "978-0-12-802444-7" not just "0-12-802444-7")
   - Validates format: 10-digit ISBNs must have 9 digits followed by a digit or 'X'; 13-digit ISBNs must be all digits
   - **Proximity filtering**: Requires 'ISBN' to appear within 6 characters before the number
   - This strict proximity check prevents false positives from other identifiers (LCCN, OCLC, etc.) that may appear in the same citation
3. **Validation**: 
   - ISBN-10: Modulo 11 checksum (with 'X' support)
   - ISBN-13: Modulo 10 checksum with alternating weights
4. **Output**: Generates text reports and CSV files for failed ISBNs

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd wiki_isbn_checksum

# Install dependencies (if using uv)
cd src
uv pip install -r requirements.txt
```

## Usage

### Basic Command

```bash
python main.py
```

### With Options

```bash
# Specify custom dumps directory
python main.py --dumps-dir /path/to/dumps

# Adjust context window around ISBNs
python main.py --context 100

# Custom output file prefix
python main.py --output-prefix wikipedia_isbn_analysis

# Full example
python main.py --dumps-dir ../dumps --context 75 --output-prefix run_2025
```

### Command Line Arguments

- `--dumps-dir`: Directory containing Wikipedia dump files (default: `../dumps`)
- `--context`: Number of context characters around ISBN (default: 50)
- `--output-prefix`: Output file prefix (default: timestamp)

## Input Format

Place Wikipedia XML dump files (`.bz2` format) in the dumps directory. Files should follow the naming pattern:
```
{lang}wiki-YYYYMMDD-pages-articles*.xml-p*p*.bz2
```

Where `{lang}` is the language code (e.g., `en` for English, `de` for German, `fr` for French).

Download dumps from: 
- English: https://dumps.wikimedia.org/enwiki/
- German: https://dumps.wikimedia.org/dewiki/
- French: https://dumps.wikimedia.org/frwiki/
- Other languages: https://dumps.wikimedia.org/{lang}wiki/

## Output Files

The tool generates two output files in the `data/` directory:

1. **Text Report** (`YYYYMMDD_HHMMSS.txt` or custom prefix):
   - Processing statistics
   - ISBN validation pass rates
   - Format breakdown (ISBN-10 vs ISBN-13)
   - Unique ISBN counts
   - Language breakdown (when processing multiple languages)

2. **CSV File** (`YYYYMMDD_HHMMSS.csv` or custom prefix):
   - Contains all failed ISBNs with:
     - Article title and URL
     - Language code
     - ISBN number
     - Format type
     - Surrounding context

## Results

In testing on Wikipedia dumps, the tool found:
- **99.3% pass rate** for ISBN checksums
- 433,761 total ISBNs across 45,401 articles
- 286,281 unique valid ISBNs
- Only 3,226 invalid checksums (0.7%)

The small percentage of failures likely represents typos in Wikipedia articles or edge cases in extraction.

## Performance

- Processes approximately 322 articles per second
- Memory efficient - handles multi-GB dump files
- Progress updates every 100 articles with ISBNs

## Requirements

- Python 3.11+
- Standard library only (no external dependencies)

## License

[Add appropriate license]
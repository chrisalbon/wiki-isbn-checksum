# Wikipedia ISBN Checksum Validator

A Python tool that processes Wikipedia XML dump files to extract and validate ISBN numbers found in articles. This tool was created to analyze the accuracy of ISBN checksums in Wikipedia content.

## Overview

This tool:
- Processes compressed Wikipedia XML dumps (.bz2 files)
- Extracts all ISBN numbers from article text
- Validates ISBN-10 and ISBN-13 checksums
- Generates detailed reports on validation results
- Exports invalid ISBNs with context for manual review

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

# Using uv (recommended)
cd src
uv pip install -e .

# Optional: Install with download capability
uv pip install -e ".[download]"

# Or using standard pip
pip install -e .
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

# Adjust ISBN proximity requirement (stricter = fewer false positives)
# Default is 6, which handles "ISBN: 978-0-12-345678-9" (2 chars) with room to spare
python main.py --proximity 4

# Enable parallel processing (use -1 for all CPU cores)
python main.py --workers 4

# Custom output file prefix
python main.py --output-prefix wikipedia_isbn_analysis

# Full example with parallel processing
python main.py --dumps-dir ../dumps --context 75 --proximity 10 --workers -1 --output-prefix run_2025
```

### Command Line Arguments

- `--dumps-dir`: Directory containing Wikipedia dump files (default: `../dumps`)
- `--context`: Number of context characters around ISBN (default: 50)
- `--proximity`: Maximum characters between end of 'ISBN' and start of number (default: 6)
- `--workers`: Number of parallel workers (-1 for all CPUs, default: 1)
- `--output-prefix`: Output file prefix (default: timestamp)

## Input Format

Place Wikipedia XML dump files (`.bz2` format) in the dumps directory. Files should follow the naming pattern:
```
{lang}wiki-YYYYMMDD-pages-articles-multistream.xml.bz2
```

Where `{lang}` is the language code (e.g., `en` for English, `de` for German, `fr` for French).

### Downloading Dumps

You can use the included `download_wiki_dumps.py` script to download dumps automatically:

```bash
python download_wiki_dumps.py
```

Or download manually from:
- English: https://dumps.wikimedia.org/enwiki/
- German: https://dumps.wikimedia.org/dewiki/
- French: https://dumps.wikimedia.org/frwiki/
- Other languages: https://dumps.wikimedia.org/{lang}wiki/

### Compatibility

The tool automatically detects and handles different XML namespace versions, making it compatible with Wikipedia dumps from any year (tested with 2021, 2022, and 2025 dumps).

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


## Performance

- Processes approximately 300-9000+ articles per second depending on content and hardware
- Supports parallel processing across multiple CPU cores
- Memory efficient - handles multi-GB dump files
- Progress updates every 100 articles with ISBNs

## Requirements

- Python 3.11+
- No external dependencies for core ISBN validation
- Optional: `requests` library for automated dump downloading

## License

MIT License

Copyright (c) 2025 Chris Albon

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
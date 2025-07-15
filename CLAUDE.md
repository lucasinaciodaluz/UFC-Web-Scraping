# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a UFC web scraping project that extracts data from ufcstats.com into structured CSV files. The scraper collects information about UFC events, fights, fight statistics, and fighters.

## Commands

### Running the Scraper
```bash
cd ufc_web_scraper
python main.py
```

### Development Commands
```bash
# Run with specific Python version if needed
python3 main.py

# Monitor log output in real-time
tail -f ufc_web_scraper/ufc_scraper.log
```

## Architecture

The project follows a modular scraping architecture with three main phases:

### Phase 1: URL Collection (`scraper/get_urls.py`)
- Collects event URLs from ufcstats.com
- Extracts fight URLs from each event page
- Gathers fighter URLs from fight pages
- Implements retry logic and rate limiting
- Saves URLs to CSV files in `urls/` directory
- Uses checkpointing system to resume interrupted URL collection

### Phase 2: Data Scraping (Individual scraper modules)
- `events.py`: Scrapes event metadata (name, date, location)
- `fights.py`: Extracts fight details and outcomes
- `fightstats.py`: Collects detailed fight statistics
- `fighters.py`: Gathers fighter biographical information
- Each module handles duplicate detection and incremental updates

### Phase 3: Data Normalization (`scraper/normalise_tables.py`)
- Cleans and standardizes scraped data
- Handles data type conversions
- Ensures consistent formatting across all CSV files

## Key Technical Details

### File Structure
- `main.py`: Entry point that orchestrates the scraping process
- `scraper/`: Contains all scraping modules
- `scraped_files/`: Output directory for final CSV files
- `urls/`: Stores collected URLs for processing
- `checkpoints/`: Progress tracking for resumable operations
- `ufc_scraper.log`: Comprehensive logging output

### Error Handling & Resilience
- Implements retry strategy with exponential backoff
- Comprehensive logging throughout the process
- Checkpoint system allows resuming interrupted scraping
- Duplicate detection prevents re-scraping existing data

### Data Flow
1. URLs are collected and saved to CSV files
2. Each scraper module reads URLs and processes them incrementally
3. Data is appended to existing CSV files (incremental updates)
4. Final normalization ensures data consistency

## Important Notes

- The scraper respects rate limits and includes delays between requests
- All scraped data is saved incrementally - running the script multiple times will only scrape new data
- The project uses BeautifulSoup for HTML parsing and requests for HTTP operations
- No external dependencies file exists - imports are handled directly in source files
- The scraper is designed to be resumable if interrupted during execution
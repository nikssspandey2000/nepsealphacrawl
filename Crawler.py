import asyncio
import pandas as pd
import re
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
import time
import os
import datetime
import numpy as np
import sys
import argparse

class DataCrawler:
    def __init__(self, sheet_name):
        self.sheet_name = sheet_name
        self.configure_logging()
        
    def configure_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("crawler_debug.log"),
                logging.StreamHandler()
            ]
        )
    
    @staticmethod
    def get_script_name():
        script_path = sys.argv[0]
        filename_with_ext = os.path.basename(script_path)
        return os.path.splitext(filename_with_ext)[0]
    
    @staticmethod
    def extract_symbol(url):
        return url.split('=')[-1].upper()
    
    @staticmethod
    def extract_tables(html, url):
        symbol = DataCrawler.extract_symbol(url)
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')
        
        logging.info(f"Found {len(tables)} tables for {symbol}")
        table_data = []
        
        for table_idx, table in enumerate(tables, 1):
            headers = []
            header_row = table.find('thead') and table.find('thead').find('tr') or table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
            
            rows = []
            body = table.find('tbody') or table
            for tr in body.find_all('tr'):
                if tr.find('th'):
                    continue
                    
                row_cells = tr.find_all('td')
                row = [cell.get_text(strip=True) for cell in row_cells]
                
                if row:
                    rows.append(row)
            
            if not rows:
                logging.info(f"Table {table_idx} for {symbol} has no data rows")
                continue
            
            if headers and len(headers) != len(rows[0]):
                headers = [f"Column_{j}" for j in range(1, len(rows[0])+1)]
            
            df = pd.DataFrame(rows, columns=headers)
            df['Table_Index'] = table_idx
            df['Symbol'] = symbol
            table_data.append(df)
            
            logging.info(f"Table {table_idx} for {symbol}: {df.shape[0]} rows, {df.shape[1]} columns")
        
        return table_data

    @staticmethod
    def save_to_csv(df, filename):
        """Save dataframe to CSV, appending if file exists"""
        try:
            if os.path.exists(filename):
                existing = pd.read_csv(filename)
                # Remove existing rows for same symbols to avoid duplicates
                if 'Symbol' in existing.columns and 'Symbol' in df.columns:
                    existing = existing[~existing['Symbol'].isin(df['Symbol'])]
                combined = pd.concat([existing, df], ignore_index=True)
                combined.to_csv(filename, index=False)
            else:
                df.to_csv(filename, index=False)
            logging.info(f"Saved to {filename}")
            return True
        except Exception as e:
            logging.error(f"Failed to save CSV {filename}: {str(e)}")
            return False

    @staticmethod
    def save_to_excel(book, excel_file):
        """Save all sheets to Excel file"""
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                for sheet_name, df in book.items():
                    # Excel sheet names max 31 chars
                    safe_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=safe_name, index=False)
            logging.info(f"Saved to {excel_file}")
            return True
        except Exception as e:
            logging.error(f"Failed to save Excel {excel_file}: {str(e)}")
            return False
    
    async def crawl_all_urls(self):
        start_time = time.time()
        df_links = pd.read_excel('NepseAlphaLink.xlsx', sheet_name=self.sheet_name)
        urls = df_links['Link'].tolist()
        logging.info(f"Found {len(urls)} URLs to crawl")
        
        excel_file = "nepsealpha.xlsx"
        sanitized_sheet = re.sub(r'[^a-zA-Z0-9]', '_', self.sheet_name)
        csv_attrs = f"{sanitized_sheet}_Attributes.csv"
        csv_add   = f"{sanitized_sheet}_Additional.csv"

        all_pivoted_data = []
        all_third_tables = []
        progress = tqdm(total=len(urls), desc="Crawling URLs")
        
        for url in urls:
            symbol = self.extract_symbol(url)
            try:
                logging.info(f"Processing {symbol} at {url}")
                
                async with AsyncWebCrawler() as crawler:
                    result = await crawler.arun(
                        url=url,
                        headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                            "Accept-Language": "en-US,en;q=0.5",
                            "Referer": "https://nepsealpha.com/",
                        },
                        wait_for_content=True,
                        wait_until="domcontentloaded",
                        timeout=60000,
                        screenshot=False
                    )
                
                if result and result.html:
                    tables = self.extract_tables(result.html, url)
                    
                    if len(tables) >= 2:
                        combined = pd.concat([tables[0], tables[1]], ignore_index=True)
                        combined.columns = [str(col).strip() for col in combined.columns]
                        
                        compare_cols = [col for col in combined.columns if str(col).startswith('Compare')]
                        if compare_cols:
                            combined = combined.drop(columns=compare_cols)
                        
                        if combined.shape[1] >= 3:
                            data_cols = [col for col in combined.columns if col not in ['Table_Index', 'Symbol']]
                            
                            if len(data_cols) >= 2:
                                attribute_col = data_cols[0]
                                value_col = data_cols[1]
                                
                                pivot_df = combined.pivot_table(
                                    index='Symbol',
                                    columns=attribute_col,
                                    values=value_col,
                                    aggfunc='first'
                                ).reset_index()
                                
                                all_pivoted_data.append(pivot_df)
                                logging.info(f"Created pivot table for {symbol}")
                    
                    if len(tables) >= 3:
                        third_table = tables[2]
                        third_table.columns = [str(col).strip() for col in third_table.columns]
                        
                        compare_cols = [col for col in third_table.columns if str(col).startswith('Compare')]
                        if compare_cols:
                            third_table = third_table.drop(columns=compare_cols)
                        
                        all_third_tables.append(third_table)
                        logging.info(f"Added third table for {symbol}")
                
                progress.update(1)
                progress.set_postfix({"Status": symbol})
                await asyncio.sleep(2)
                    
            except Exception as e:
                logging.error(f"Error processing {symbol}: {str(e)}")
                progress.update(1)
                progress.set_postfix({"Error": symbol})
                await asyncio.sleep(2)
        
        progress.close()

        # ── Build book dict for Excel ──────────────────────────────────────
        book = {}
        if os.path.exists(excel_file):
            try:
                book = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')
            except Exception as e:
                logging.warning(f"Could not read existing Excel: {e}. Starting fresh.")
                book = {}

        sheet_name_attrs = f"{sanitized_sheet}_Attributes"
        sheet_name_add   = f"{sanitized_sheet}_Additional"

        # ── Attributes / pivot data ────────────────────────────────────────
        if all_pivoted_data:
            pivoted_combined = pd.concat(all_pivoted_data, ignore_index=True)
            pivoted_combined.columns = [str(col).strip() for col in pivoted_combined.columns]
            compare_cols = [col for col in pivoted_combined.columns if str(col).startswith('Compare')]
            if compare_cols:
                pivoted_combined = pivoted_combined.drop(columns=compare_cols)

            book[sheet_name_attrs] = pivoted_combined
            logging.info(f"Prepared {sheet_name_attrs} with {pivoted_combined.shape[0]} rows")

            # Save CSV
            self.save_to_csv(pivoted_combined, csv_attrs)
        else:
            logging.warning("No pivot/attributes data collected!")

        # ── Additional / third-table data ──────────────────────────────────
        if all_third_tables:
            third_tables_combined = pd.concat(all_third_tables, ignore_index=True)
            third_tables_combined.columns = [str(col).strip() for col in third_tables_combined.columns]

            if 'Table_Index' in third_tables_combined.columns:
                third_tables_combined = third_tables_combined.drop(columns=['Table_Index'])
            if 'Symbol' in third_tables_combined.columns:
                cols = ['Symbol'] + [c for c in third_tables_combined.columns if c != 'Symbol']
                third_tables_combined = third_tables_combined[cols]

            book[sheet_name_add] = third_tables_combined
            logging.info(f"Prepared {sheet_name_add} with {third_tables_combined.shape[0]} rows")

            # Save CSV
            self.save_to_csv(third_tables_combined, csv_add)
        else:
            logging.warning("No additional/third-table data collected!")

        # ── Write Excel ────────────────────────────────────────────────────
        if book:
            success = self.save_to_excel(book, excel_file)
            if success:
                print(f"\n✅ Excel saved: {excel_file}")
            else:
                print(f"\n❌ Excel save failed — check crawler_debug.log")
        else:
            logging.warning("No data to save!")

        # ── Summary ────────────────────────────────────────────────────────
        total_time = time.time() - start_time
        mins, secs = divmod(total_time, 60)
        logging.info(f"Total execution time: {int(mins)} minutes {secs:.2f} seconds")
        print(f"Total execution time: {int(mins)} minutes {secs:.2f} seconds")

        # Print saved files
        print("\nFiles saved:")
        for f in [excel_file, csv_attrs, csv_add]:
            if os.path.exists(f):
                size = os.path.getsize(f) / 1024
                print(f"  ✅ {f}  ({size:.1f} KB)")
            else:
                print(f"  ❌ {f}  (not created)")
        
        return True


def run_crawler(sheet_name):
    os.makedirs("html", exist_ok=True)
    print("Starting crawling process...")
    logging.info("Starting crawling process")
    start_time = time.time()
    
    try:
        crawler = DataCrawler(sheet_name)
        asyncio.run(crawler.crawl_all_urls())
        print("Crawling completed successfully!")
        logging.info("Crawling completed successfully")
    except Exception as e:
        print(f"Crawling failed: {str(e)}")
        logging.error(f"Crawling failed: {str(e)}")
    
    total_time = time.time() - start_time
    mins, secs = divmod(total_time, 60)
    print(f"Total execution time: {int(mins)} minutes {secs:.2f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Data Crawler with specified sheet name')
    parser.add_argument('sheet_name', nargs='?', default='Link',
                        help='Sheet name in NepseAlphaLink.xlsx to process (default: Link)')
    
    args = parser.parse_args()
    run_crawler(args.sheet_name)

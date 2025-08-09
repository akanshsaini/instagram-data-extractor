
"""
Persistent Instagram Data Extractor with Aggressive Retry Logic
Author: AI Assistant
Date: August 2025
Purpose: Never gives up until ALL URLs are successfully processed
"""

import instaloader
import gspread
import json
import os
import re
import time
import logging
from datetime import datetime
import pytz
from google.oauth2.service_account import Credentials
from typing import Optional, Dict, Any, List, Tuple
import random

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class PersistentInstagramExtractor:
    """Instagram extractor that NEVER gives up on URLs"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.instagram_loader = None
        self.google_sheet = None
        self.worksheet = None
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.max_retries_per_url = 5
        self.max_total_attempts = 3  # Retry entire batch this many times

        self._initialize_services()

    def _initialize_services(self):
        """Initialize services"""
        try:
            self.instagram_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True
            )

            logger.info("✅ Instagram service initialized")

            # Initialize Google Sheets
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]

            creds_dict = json.loads(self.credentials_json)
            credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)

            gc = gspread.authorize(credentials)
            self.google_sheet = gc.open_by_key(self.sheet_id)
            self.worksheet = self.google_sheet.get_worksheet(0)

            self._setup_headers()
            logger.info("✅ Google Sheets service initialized")

        except Exception as e:
            logger.error(f"❌ Service initialization failed: {e}")
            raise

    def _setup_headers(self):
        """Setup headers"""
        headers = [
            'Instagram URL', 'Account Handle', 'Likes Count', 'Comments Count', 
            'Views Count', 'Content Type', 'Posted Date', 'Caption Text', 
            'Hashtags Count', 'Location', 'Last Fetched IST', 'Processing Status', 
            'Last Updated IST', 'Retry Count'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or len(current_headers) < len(headers):
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Clean header formatting
                self.worksheet.format('A1:N1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.3, 'blue': 0.6},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                })

                logger.info("✅ Headers configured with retry tracking")

        except Exception as e:
            logger.error(f"❌ Header setup failed: {e}")

    def _get_ist_timestamp(self) -> str:
        """Get current IST timestamp"""
        return datetime.now(self.ist_timezone).strftime('%d/%m/%Y %H:%M:%S IST')

    def _extract_shortcode(self, url: str) -> Optional[str]:
        """Extract shortcode from Instagram URL"""
        url = url.strip()
        patterns = [
            r'/p/([A-Za-z0-9_-]+)',
            r'/reel/([A-Za-z0-9_-]+)', 
            r'/tv/([A-Za-z0-9_-]+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def _create_fresh_loader(self):
        """Create a fresh Instagram loader for retry attempts"""
        try:
            fresh_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True
            )

            # Randomize user agent slightly
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            ]
            fresh_loader.context.user_agent = random.choice(user_agents)

            return fresh_loader
        except:
            return self.instagram_loader

    def _extract_post_data_with_retries(self, url: str, retry_count: int = 0) -> tuple[Optional[Dict[str, Any]], str, int]:
        """Extract post data with aggressive retry logic"""
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return None, "INVALID_URL", retry_count

        for attempt in range(self.max_retries_per_url):
            try:
                logger.info(f"Attempt {attempt + 1}/{self.max_retries_per_url} for {shortcode}")

                # Use fresh loader for each retry after first attempt
                if attempt == 0:
                    loader = self.instagram_loader
                else:
                    loader = self._create_fresh_loader()
                    # Progressive delay
                    delay = (attempt + 1) * 2 + random.uniform(1, 3)
                    logger.info(f"Waiting {delay:.1f} seconds before retry...")
                    time.sleep(delay)

                post = instaloader.Post.from_shortcode(loader.context, shortcode)

                # Extract data
                current_ist = self._get_ist_timestamp()
                posted_ist = post.date.astimezone(self.ist_timezone).strftime('%d/%m/%Y %H:%M IST') if post.date else 'Unknown'

                data = {
                    'account': post.owner_username,
                    'likes': post.likes if post.likes else 0,
                    'comments': post.comments if post.comments else 0,
                    'views': post.video_view_count if (post.is_video and hasattr(post, 'video_view_count') and post.video_view_count) else 0,
                    'type': 'Video/Reel' if post.is_video else 'Photo',
                    'posted_date': posted_ist,
                    'caption': self._clean_caption(post.caption),
                    'hashtags': len(re.findall(r'#\w+', post.caption)) if post.caption else 0,
                    'location': post.location.name if post.location else 'Not specified',
                    'last_fetched': current_ist,
                    'last_updated': current_ist
                }

                logger.info(f"✅ SUCCESS on attempt {attempt + 1}: @{data['account']}")
                return data, "SUCCESS", retry_count + attempt + 1

            except instaloader.exceptions.TooManyRequestsException:
                logger.warning(f"Rate limited on attempt {attempt + 1}, waiting longer...")
                time.sleep(60)  # Wait 1 minute for rate limits
                continue
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                continue

        logger.error(f"❌ All {self.max_retries_per_url} attempts failed for {shortcode}")
        return None, "ALL_RETRIES_FAILED", retry_count + self.max_retries_per_url

    def _clean_caption(self, caption: str) -> str:
        """Clean caption text"""
        if not caption:
            return "No caption"

        cleaned = ' '.join(str(caption).split())
        if len(cleaned) > 150:
            return cleaned[:150] + "..."
        return cleaned

    def _update_sheet_row(self, row_num: int, url: str, data: Optional[Dict], status: str, retry_count: int):
        """Update spreadsheet row with retry tracking"""
        try:
            current_ist = self._get_ist_timestamp()

            if data and status == "SUCCESS":
                likes_formatted = f"{data['likes']:,}" if isinstance(data['likes'], int) else str(data['likes'])
                comments_formatted = f"{data['comments']:,}" if isinstance(data['comments'], int) else str(data['comments'])
                views_formatted = f"{data['views']:,}" if isinstance(data['views'], int) else str(data['views'])

                row_data = [
                    url,
                    f"@{data['account']}",
                    likes_formatted,
                    comments_formatted,
                    views_formatted,
                    data['type'],
                    data['posted_date'],
                    data['caption'],
                    str(data['hashtags']),
                    data['location'],
                    data['last_fetched'],
                    "✅ SUCCESS",
                    current_ist,
                    str(retry_count)
                ]

                # Green background for success
                self.worksheet.format(f'A{row_num}:N{row_num}', {
                    'backgroundColor': {'red': 0.9, 'green': 1.0, 'blue': 0.9}
                })

            else:
                status_message = "❌ Failed after all retries" if status == "ALL_RETRIES_FAILED" else "❌ Processing failed"
                row_data = [url] + [''] * 11 + [status_message, current_ist, str(retry_count)]

                # Red background for failure
                self.worksheet.format(f'A{row_num}:N{row_num}', {
                    'backgroundColor': {'red': 1.0, 'green': 0.9, 'blue': 0.9}
                })

            self.worksheet.update(f'A{row_num}:N{row_num}', [row_data])
            return True

        except Exception as e:
            logger.error(f"Row update failed for row {row_num}: {e}")
            return False

    def _collect_failed_urls(self, all_data: List[List[str]]) -> List[Tuple[int, str]]:
        """Collect URLs that need processing or re-processing"""
        failed_urls = []

        for row_index, row in enumerate(all_data[1:], start=2):
            if row and len(row) > 0 and row[0].strip():
                url = row[0].strip()
                if url.startswith('http') and 'instagram.com' in url:
                    # Check if this URL needs processing
                    status = row[11] if len(row) > 11 else ""

                    # Always process URLs OR process failed ones
                    if "✅ SUCCESS" not in status:
                        failed_urls.append((row_index, url))

        return failed_urls

    def process_urls_persistently(self) -> int:
        """Process URLs with persistent retry until ALL succeed"""
        logger.info("🚀 Starting PERSISTENT Instagram data extraction")
        logger.info("💪 Will keep trying until ALL URLs are successful!")

        total_successful = 0

        for batch_attempt in range(self.max_total_attempts):
            logger.info(f"📊 BATCH ATTEMPT {batch_attempt + 1}/{self.max_total_attempts}")

            try:
                all_data = self.worksheet.get_all_values()

                if len(all_data) <= 1:
                    logger.info("📝 No URLs found for processing")
                    return 0

                # Collect URLs that need processing
                failed_urls = self._collect_failed_urls(all_data)

                if not failed_urls:
                    logger.info("🎉 All URLs are already successful!")
                    return total_successful

                logger.info(f"🔄 Found {len(failed_urls)} URLs to process")

                batch_successful = 0

                # Process each failed URL
                for i, (row_index, url) in enumerate(failed_urls, 1):
                    try:
                        logger.info(f"📱 Processing URL {i}/{len(failed_urls)}: {url}")

                        data, status, retry_count = self._extract_post_data_with_retries(url)

                        if self._update_sheet_row(row_index, url, data, status, retry_count):
                            if status == "SUCCESS":
                                batch_successful += 1
                                logger.info(f"✅ Success: @{data['account']} - {data['likes']} likes")
                            else:
                                logger.warning(f"❌ Failed after all retries: {url}")

                        # Rate limiting between URLs
                        if i < len(failed_urls):
                            time.sleep(random.uniform(2, 4))

                    except Exception as e:
                        logger.error(f"❌ Error processing URL {url}: {e}")
                        continue

                total_successful += batch_successful
                logger.info(f"📊 Batch {batch_attempt + 1} complete: {batch_successful} URLs successful")

                # Check if we're done
                remaining_failed = len(failed_urls) - batch_successful
                if remaining_failed == 0:
                    logger.info("🎉 ALL URLs successfully processed!")
                    break
                else:
                    logger.info(f"🔄 {remaining_failed} URLs still need processing...")
                    if batch_attempt < self.max_total_attempts - 1:
                        logger.info("⏳ Waiting before next batch attempt...")
                        time.sleep(30)  # Wait 30 seconds between batch attempts

            except Exception as e:
                logger.error(f"❌ Batch attempt {batch_attempt + 1} failed: {e}")
                if batch_attempt < self.max_total_attempts - 1:
                    time.sleep(60)  # Wait 1 minute before retry
                continue

        logger.info(f"🎯 Final result: {total_successful} URLs successfully processed")
        return total_successful

def main():
    """Main function"""
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id or not credentials_json:
        logger.error("❌ Environment variables not found")
        return

    try:
        logger.info("🚀 Initializing PERSISTENT Instagram Extractor")
        extractor = PersistentInstagramExtractor(sheet_id, credentials_json)

        processed = extractor.process_urls_persistently()

        logger.info(f"✅ PERSISTENT extraction completed!")
        logger.info(f"📊 Total successful extractions: {processed}")

    except Exception as e:
        logger.error(f"❌ Application error: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()

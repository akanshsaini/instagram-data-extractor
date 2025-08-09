
"""
Working Professional Instagram Data Extractor
Author: AI Assistant
Date: August 2025
Purpose: Fixed version that actually works
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
from typing import Optional, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkingInstagramExtractor:
    """Working Instagram data extractor - fixed version"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.instagram_loader = None
        self.google_sheet = None
        self.worksheet = None
        self.ist_timezone = pytz.timezone('Asia/Kolkata')

        self._initialize_services()

    def _initialize_services(self):
        """Initialize services with ONLY working parameters"""
        try:
            # Initialize Instagram loader with ONLY basic, working parameters
            self.instagram_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True
            )

            logger.info("✅ Instagram service initialized (working version)")

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

            self._setup_clean_headers()
            logger.info("✅ Google Sheets service initialized (working version)")

        except Exception as e:
            logger.error(f"❌ Service initialization failed: {e}")
            raise

    def _setup_clean_headers(self):
        """Setup clean headers without breaking anything"""
        headers = [
            'Instagram URL', 'Account Handle', 'Likes Count', 'Comments Count', 
            'Views Count', 'Content Type', 'Posted Date', 'Caption Text', 
            'Hashtags Count', 'Location', 'Last Fetched IST', 'Processing Status', 'Last Updated IST'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or len(current_headers) < len(headers):
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Simple, working header formatting
                self.worksheet.format('A1:M1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.3, 'blue': 0.6},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                })

                logger.info("✅ Clean headers configured")

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

    def _extract_post_data(self, url: str) -> tuple[Optional[Dict[str, Any]], str]:
        """Extract post data with working methods only"""
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return None, "INVALID_URL"

        try:
            logger.info(f"Extracting data for: {shortcode}")

            post = instaloader.Post.from_shortcode(self.instagram_loader.context, shortcode)

            # Get IST timestamps
            current_ist = self._get_ist_timestamp()
            posted_ist = post.date.astimezone(self.ist_timezone).strftime('%d/%m/%Y %H:%M IST') if post.date else 'Unknown'

            # Extract data safely
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

            logger.info(f"✅ Successfully extracted data for @{data['account']}")
            return data, "SUCCESS"

        except Exception as e:
            logger.error(f"Extraction failed for {shortcode}: {str(e)}")
            return None, "EXTRACTION_ERROR"

    def _clean_caption(self, caption: str) -> str:
        """Clean caption text"""
        if not caption:
            return "No caption"

        cleaned = ' '.join(str(caption).split())
        if len(cleaned) > 150:
            return cleaned[:150] + "..."
        return cleaned

    def _update_sheet_row(self, row_num: int, url: str, data: Optional[Dict], status: str):
        """Update spreadsheet row"""
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
                    "SUCCESS",
                    current_ist
                ]
            else:
                error_messages = {
                    "INVALID_URL": "Invalid URL format",
                    "EXTRACTION_ERROR": "Processing failed"
                }
                error_msg = error_messages.get(status, "Processing failed")
                row_data = [url] + [''] * 10 + [error_msg, current_ist]

            self.worksheet.update(f'A{row_num}:M{row_num}', [row_data])
            return True

        except Exception as e:
            logger.error(f"Row update failed for row {row_num}: {e}")
            return False

    def process_all_urls(self) -> int:
        """Process all URLs - ALWAYS refresh for real-time data"""
        logger.info("🚀 Starting working Instagram data extraction")

        try:
            all_data = self.worksheet.get_all_values()

            if len(all_data) <= 1:
                logger.info("📝 No URLs found for processing")
                return 0

            processed_count = 0
            urls_to_process = []

            # Collect all URLs
            for row_index, row in enumerate(all_data[1:], start=2):
                if row and len(row) > 0 and row[0].strip():
                    url = row[0].strip()
                    if url.startswith('http') and 'instagram.com' in url:
                        urls_to_process.append((row_index, url))

            total_urls = len(urls_to_process)
            logger.info(f"📊 Found {total_urls} Instagram URLs to process")

            if total_urls == 0:
                return 0

            # Process each URL with error handling
            for i, (row_index, url) in enumerate(urls_to_process, 1):
                try:
                    logger.info(f"📱 Processing URL {i}/{total_urls}: {url}")

                    data, status = self._extract_post_data(url)

                    if self._update_sheet_row(row_index, url, data, status):
                        processed_count += 1

                        if status == "SUCCESS":
                            logger.info(f"✅ Success: @{data['account']} - {data['likes']} likes")
                        else:
                            logger.warning(f"❌ Failed: {status}")

                    # Simple rate limiting
                    if i < total_urls:
                        time.sleep(3)

                except Exception as e:
                    logger.error(f"❌ Error processing URL {url}: {e}")
                    continue

            logger.info(f"🎉 Processing complete! Processed: {processed_count} URLs")
            return processed_count

        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            return 0

def main():
    """Main function"""
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id:
        logger.error("❌ SHEET_ID environment variable not found")
        return

    if not credentials_json:
        logger.error("❌ CREDENTIALS_JSON environment variable not found")
        return

    try:
        logger.info("🚀 Initializing Working Instagram Extractor")
        extractor = WorkingInstagramExtractor(sheet_id, credentials_json)

        processed = extractor.process_all_urls()

        logger.info(f"✅ Extraction completed! Processed: {processed}")

    except Exception as e:
        logger.error(f"❌ Application error: {e}")

if __name__ == "__main__":
    main()

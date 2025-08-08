"""
Instagram Data Extractor - Professional Edition
Author: AI Assistant
Version: 1.0
Purpose: Extract Instagram post data and save to Google Sheets automatically
"""

import instaloader
import gspread
import json
import os
import re
import time
import logging
from datetime import datetime
from google.oauth2.service_account import Credentials
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramDataExtractor:
    """Professional Instagram data extraction service"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.instagram_loader = None
        self.google_sheet = None
        self.worksheet = None

        self._initialize_services()

    def _initialize_services(self):
        """Initialize Instagram and Google Sheets services"""
        try:
            # Initialize Instagram loader
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

            self._setup_sheet_headers()
            logger.info("✅ Google Sheets service initialized")

        except Exception as e:
            logger.error(f"❌ Service initialization failed: {e}")
            raise

    def _setup_sheet_headers(self):
        """Setup professional sheet headers"""
        headers = [
            'Instagram URL', 'Account Name', 'Likes Count', 'Comments Count', 
            'Views Count', 'Content Type', 'Posted Date', 'Caption Text', 
            'Hashtags Count', 'Location', 'Processing Time', 'Status'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or current_headers[0] != headers[0]:
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Format headers professionally
                self.worksheet.format('A1:L1', {
                    'backgroundColor': {'red': 0.1, 'green': 0.3, 'blue': 0.7},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                })

                logger.info("✅ Sheet headers configured")

        except Exception as e:
            logger.error(f"❌ Header setup failed: {e}")

    def extract_shortcode(self, url: str) -> Optional[str]:
        """Extract Instagram shortcode from URL"""
        patterns = [
            r'/p/([A-Za-z0-9_-]+)',
            r'/reel/([A-Za-z0-9_-]+)', 
            r'/tv/([A-Za-z0-9_-]+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, url.strip())
            if match:
                return match.group(1)
        return None

    def extract_post_data(self, url: str) -> tuple[Optional[Dict[str, Any]], str]:
        """Extract comprehensive data from Instagram post"""
        try:
            shortcode = self.extract_shortcode(url)
            if not shortcode:
                return None, "INVALID_URL"

            post = instaloader.Post.from_shortcode(self.instagram_loader.context, shortcode)

            # Extract comprehensive post data
            post_data = {
                'account_name': f"@{post.owner_username}",
                'likes_count': f"{post.likes:,}",
                'comments_count': f"{post.comments:,}",
                'views_count': f"{post.video_view_count:,}" if post.is_video and hasattr(post, 'video_view_count') else "0",
                'content_type': 'Video/Reel' if post.is_video else 'Photo',
                'posted_date': post.date.strftime('%Y-%m-%d %H:%M:%S'),
                'caption_text': self._clean_caption(post.caption),
                'hashtags_count': len(re.findall(r'#\w+', post.caption)) if post.caption else 0,
                'location': post.location.name if post.location else 'Not specified',
                'processing_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            return post_data, "SUCCESS"

        except instaloader.exceptions.PostUnavailableException:
            return None, "POST_NOT_FOUND"
        except instaloader.exceptions.LoginRequiredException:
            return None, "LOGIN_REQUIRED"
        except Exception as e:
            logger.error(f"Data extraction failed for {url}: {e}")
            return None, "EXTRACTION_ERROR"

    def _clean_caption(self, caption: str) -> str:
        """Clean and format caption text"""
        if not caption:
            return "No caption"

        # Remove excessive whitespace and newlines
        cleaned = ' '.join(caption.split())

        # Truncate if too long for spreadsheet display
        if len(cleaned) > 150:
            return cleaned[:150] + "..."

        return cleaned

    def update_sheet_row(self, row_number: int, url: str, data: Optional[Dict], status: str):
        """Update spreadsheet row with extracted data"""
        try:
            if data and status == "SUCCESS":
                row_data = [
                    url,
                    data['account_name'],
                    data['likes_count'],
                    data['comments_count'],
                    data['views_count'],
                    data['content_type'],
                    data['posted_date'],
                    data['caption_text'],
                    data['hashtags_count'],
                    data['location'],
                    data['processing_time'],
                    "✅ Success"
                ]
            else:
                # Keep URL, clear other data, show error status
                status_messages = {
                    "INVALID_URL": "❌ Invalid URL format",
                    "POST_NOT_FOUND": "❌ Post not found or private",
                    "LOGIN_REQUIRED": "❌ Login required",
                    "EXTRACTION_ERROR": "❌ Processing error"
                }
                row_data = [url] + [''] * 10 + [status_messages.get(status, "❌ Unknown error")]

            self.worksheet.update(f'A{row_number}:L{row_number}', [row_data])
            return True

        except Exception as e:
            logger.error(f"Sheet update failed for row {row_number}: {e}")
            return False

    def process_all_urls(self) -> int:
        """Process all URLs in the spreadsheet"""
        logger.info("🚀 Starting data extraction process")

        try:
            all_data = self.worksheet.get_all_values()

            if len(all_data) <= 1:
                logger.info("📝 No URLs found for processing")
                return 0

            processed_count = 0

            for row_index, row in enumerate(all_data[1:], start=2):
                if not row or not row[0].strip():
                    continue

                url = row[0].strip()

                # Skip if already processed successfully
                if len(row) >= 12 and "✅ Success" in str(row[11]):
                    continue

                logger.info(f"📱 Processing: {url}")

                # Extract Instagram data
                data, status = self.extract_post_data(url)

                # Update spreadsheet
                if self.update_sheet_row(row_index, url, data, status):
                    processed_count += 1

                    if status == "SUCCESS":
                        logger.info(f"✅ {data['account_name']}: {data['likes_count']} likes, {data['comments_count']} comments")
                    else:
                        logger.warning(f"⚠️ Failed to process: {status}")

                # Rate limiting delay
                time.sleep(2)

            logger.info(f"🎉 Processing complete: {processed_count} URLs processed")
            return processed_count

        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            return 0

def main():
    """Main execution function"""
    # Get configuration from environment variables
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id:
        logger.error("❌ SHEET_ID environment variable not found")
        return

    if not credentials_json:
        logger.error("❌ CREDENTIALS_JSON environment variable not found")
        return

    try:
        extractor = InstagramDataExtractor(sheet_id, credentials_json)
        processed_count = extractor.process_all_urls()

        logger.info(f"✅ Extraction completed successfully")
        logger.info(f"📊 Total URLs processed: {processed_count}")

    except Exception as e:
        logger.error(f"❌ Application error: {e}")

if __name__ == "__main__":
    main()

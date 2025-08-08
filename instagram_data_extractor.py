
# 🚀 REAL-TIME INSTAGRAM DATA FETCHER
# Instant refresh when you manually trigger it!

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

class RealTimeInstagramExtractor:
    """Real-time Instagram data extraction with instant processing"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.instagram_loader = None
        self.google_sheet = None
        self.worksheet = None
        self.force_refresh = os.environ.get('FORCE_REFRESH', 'false').lower() == 'true'

        self._initialize_services()

    def _initialize_services(self):
        """Initialize Instagram and Google Sheets services"""
        try:
            # Initialize Instagram loader with faster settings
            self.instagram_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True,
                compress_json=False,
                max_connection_attempts=3  # Faster connection attempts
            )
            logger.info("⚡ Instagram service initialized (fast mode)")

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
            logger.info("⚡ Google Sheets service initialized (fast mode)")

        except Exception as e:
            logger.error(f"❌ Service initialization failed: {e}")
            raise

    def _setup_sheet_headers(self):
        """Setup professional sheet headers with refresh indicator"""
        headers = [
            '📱 Instagram URL', '👤 Account', '❤️ Likes', '💬 Comments', 
            '👁️ Views', '🎬 Type', '📅 Posted Date', '📝 Caption', 
            '🏷️ Hashtags', '📍 Location', '⚡ Last Updated', '✅ Status'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or current_headers[0] != headers[0]:
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Format headers with real-time indicator
                self.worksheet.format('A1:L1', {
                    'backgroundColor': {'red': 0.0, 'green': 0.8, 'blue': 0.4},  # Green for real-time
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                })

                # Add refresh instructions
                self.worksheet.update('N1', '🔄 Manual Refresh: Run GitHub Action')
                self.worksheet.format('N1', {
                    'backgroundColor': {'red': 1.0, 'green': 0.6, 'blue': 0.0},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}
                })

                logger.info("⚡ Real-time sheet headers configured")

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
        """Extract comprehensive data from Instagram post (optimized for speed)"""
        try:
            shortcode = self.extract_shortcode(url)
            if not shortcode:
                return None, "INVALID_URL"

            post = instaloader.Post.from_shortcode(self.instagram_loader.context, shortcode)

            # Extract data with real-time timestamp
            current_time = datetime.now()
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
                'last_updated': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                'processing_time': current_time.strftime('%Y-%m-%d %H:%M:%S')
            }

            return post_data, "SUCCESS"

        except Exception as e:
            logger.error(f"Data extraction failed for {url}: {e}")
            return None, "EXTRACTION_ERROR"

    def _clean_caption(self, caption: str) -> str:
        """Clean and format caption text"""
        if not caption:
            return "No caption"

        cleaned = ' '.join(caption.split())
        if len(cleaned) > 100:
            return cleaned[:100] + "..."
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
                    data['last_updated'],
                    "⚡ Fresh Data"
                ]
            else:
                status_messages = {
                    "INVALID_URL": "❌ Invalid URL",
                    "POST_NOT_FOUND": "❌ Not found",
                    "LOGIN_REQUIRED": "❌ Private post",
                    "EXTRACTION_ERROR": "❌ Error"
                }
                row_data = [url] + [''] * 10 + [status_messages.get(status, "❌ Error")]

            self.worksheet.update(f'A{row_number}:L{row_number}', [row_data])
            return True

        except Exception as e:
            logger.error(f"Sheet update failed for row {row_number}: {e}")
            return False

    def process_all_urls_realtime(self) -> int:
        """Process all URLs with real-time refresh capability"""
        logger.info("⚡ Starting REAL-TIME data extraction")

        try:
            all_data = self.worksheet.get_all_values()

            if len(all_data) <= 1:
                logger.info("📝 No URLs found for processing")
                return 0

            processed_count = 0
            total_urls = 0

            # Count total URLs first
            for row in all_data[1:]:
                if row and row[0].strip():
                    total_urls += 1

            logger.info(f"🔍 Found {total_urls} URLs to process")

            # Process each URL with minimal delay for real-time feel
            for row_index, row in enumerate(all_data[1:], start=2):
                if not row or not row[0].strip():
                    continue

                url = row[0].strip()

                # Process ALL URLs if force refresh, or skip already processed ones
                should_process = self.force_refresh or (len(row) < 12 or "⚡ Fresh Data" not in str(row[11]))

                if not should_process:
                    continue

                logger.info(f"⚡ Processing URL {processed_count + 1}/{total_urls}: {url}")

                # Extract Instagram data with minimal delay
                data, status = self.extract_post_data(url)

                # Update spreadsheet immediately
                if self.update_sheet_row(row_index, url, data, status):
                    processed_count += 1

                    if status == "SUCCESS":
                        logger.info(f"✅ {data['account_name']}: {data['likes_count']} likes, {data['comments_count']} comments")
                    else:
                        logger.warning(f"⚠️ Failed: {status}")

                # Minimal delay for real-time processing (0.5 seconds instead of 2)
                time.sleep(0.5)

            # Update refresh timestamp
            self.worksheet.update('N2', f'Last Refresh: {datetime.now().strftime("%H:%M:%S")}')

            logger.info(f"⚡ REAL-TIME processing complete: {processed_count} URLs updated")
            return processed_count

        except Exception as e:
            logger.error(f"❌ Real-time processing failed: {e}")
            return 0

def main():
    """Main execution function for real-time processing"""
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id or not credentials_json:
        logger.error("❌ Required environment variables not found")
        return

    try:
        extractor = RealTimeInstagramExtractor(sheet_id, credentials_json)
        processed_count = extractor.process_all_urls_realtime()

        logger.info(f"⚡ REAL-TIME extraction completed!")
        logger.info(f"📊 URLs processed: {processed_count}")

        if processed_count > 0:
            logger.info("🎉 Fresh Instagram data is now available in your sheet!")

    except Exception as e:
        logger.error(f"❌ Application error: {e}")

if __name__ == "__main__":
    main()

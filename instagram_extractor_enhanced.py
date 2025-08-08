
"""
Instagram Data Extractor - GitHub Actions Compatible
Author: AI Assistant  
Date: August 2025
Purpose: Extract Instagram data for GitHub Actions deployment
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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramExtractor:
    """Instagram data extraction service"""

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
            # Initialize Instagram loader with robust settings
            self.instagram_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True,
                max_connection_attempts=3,
                request_timeout=20
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
        """Setup sheet headers"""
        headers = [
            '📱 Instagram URL', '👤 Account', '❤️ Likes', '💬 Comments', 
            '👁️ Views', '🎬 Type', '📅 Posted', '📝 Caption', 
            '🏷️ Hashtags', '📍 Location', '⚡ Updated', '✅ Status'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or current_headers[0] != headers[0]:
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Format headers
                self.worksheet.format('A1:L1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 1.0},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                })

                logger.info("✅ Headers configured")

        except Exception as e:
            logger.error(f"❌ Header setup failed: {e}")

    def _extract_shortcode(self, url: str) -> Optional[str]:
        """Extract shortcode from Instagram URL with multiple patterns"""
        url = url.strip()

        patterns = [
            r'/p/([A-Za-z0-9_-]+)',
            r'/reel/([A-Za-z0-9_-]+)', 
            r'/tv/([A-Za-z0-9_-]+)',
            r'/reels/([A-Za-z0-9_-]+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        # Try alternative extraction
        if '?igshid=' in url or '?igsh=' in url:
            base_url = url.split('?')[0]
            for pattern in patterns:
                match = re.search(pattern, base_url)
                if match:
                    return match.group(1)

        return None

    def _extract_post_data(self, url: str) -> tuple[Optional[Dict[str, Any]], str]:
        """Extract data from Instagram post with retry logic"""
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return None, "INVALID_URL"

        # Try multiple extraction strategies
        for attempt in range(3):
            try:
                if attempt > 0:
                    time.sleep(2 * attempt)  # Progressive delay

                post = instaloader.Post.from_shortcode(self.instagram_loader.context, shortcode)

                # Extract comprehensive data
                current_time = datetime.now()
                data = {
                    'account': f"@{post.owner_username}",
                    'likes': f"{post.likes:,}" if post.likes else "0",
                    'comments': f"{post.comments:,}" if post.comments else "0",
                    'views': f"{post.video_view_count:,}" if post.is_video and hasattr(post, 'video_view_count') and post.video_view_count else "0",
                    'type': 'Video/Reel' if post.is_video else 'Photo',
                    'posted_date': post.date.strftime('%m/%d/%Y %H:%M') if post.date else 'Unknown',
                    'caption': self._clean_caption(post.caption),
                    'hashtags': len(re.findall(r'#\w+', post.caption)) if post.caption else 0,
                    'location': post.location.name if post.location else 'No location',
                    'updated': current_time.strftime('%m/%d/%Y %H:%M:%S')
                }

                return data, "SUCCESS"

            except instaloader.exceptions.PostUnavailableException:
                return None, "POST_NOT_FOUND"
            except instaloader.exceptions.LoginRequiredException:
                return None, "LOGIN_REQUIRED" 
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == 2:  # Last attempt
                    return None, "EXTRACTION_ERROR"

        return None, "EXTRACTION_ERROR"

    def _clean_caption(self, caption: str) -> str:
        """Clean caption text"""
        if not caption:
            return "No caption"

        cleaned = ' '.join(str(caption).split())
        if len(cleaned) > 100:
            return cleaned[:100] + "..."
        return cleaned

    def _update_row(self, row_num: int, url: str, data: Optional[Dict], status: str):
        """Update spreadsheet row"""
        try:
            if data and status == "SUCCESS":
                row_data = [
                    url, data['account'], data['likes'], data['comments'],
                    data['views'], data['type'], data['posted_date'], data['caption'],
                    data['hashtags'], data['location'], data['updated'], "✅ Success"
                ]
            else:
                # Detailed error messages
                error_messages = {
                    "INVALID_URL": "❌ Invalid URL format",
                    "POST_NOT_FOUND": "❌ Post not found or private",
                    "LOGIN_REQUIRED": "❌ Login required",
                    "EXTRACTION_ERROR": "❌ Extraction failed"
                }
                error_msg = error_messages.get(status, "❌ Processing error")
                row_data = [url] + [''] * 10 + [error_msg]

            self.worksheet.update(f'A{row_num}:L{row_num}', [row_data])
            return True

        except Exception as e:
            logger.error(f"Row update failed: {e}")
            return False

    def process_all_urls(self) -> int:
        """Process all URLs in the spreadsheet"""
        logger.info("🚀 Starting data extraction")

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

                # Skip already processed successful URLs
                if len(row) >= 12 and "✅ Success" in str(row[11]):
                    continue

                logger.info(f"📱 Processing: {url}")

                # Extract data
                data, status = self._extract_post_data(url)

                # Update sheet
                if self._update_row(row_index, url, data, status):
                    processed_count += 1

                    if status == "SUCCESS":
                        logger.info(f"✅ {data['account']}: {data['likes']} likes, {data['comments']} comments")
                    else:
                        logger.warning(f"❌ Failed: {status}")

                # Rate limiting
                time.sleep(2)

            logger.info(f"🎉 Processing complete: {processed_count} URLs processed")
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
        extractor = InstagramExtractor(sheet_id, credentials_json)
        processed = extractor.process_all_urls()

        logger.info(f"✅ Extraction completed successfully!")
        logger.info(f"📊 Total URLs processed: {processed}")

    except Exception as e:
        logger.error(f"❌ Application error: {e}")

if __name__ == "__main__":
    main()

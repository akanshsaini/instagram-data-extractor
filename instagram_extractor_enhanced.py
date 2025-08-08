
"""
Instagram Data Extractor - Enhanced with Instant Refresh
Author: AI Assistant  
Date: August 2025
Purpose: Extract Instagram data with instant refresh capability
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

class InstagramExtractorEnhanced:
    """Enhanced Instagram data extraction with instant refresh support"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.instagram_loader = None
        self.google_sheet = None
        self.worksheet = None
        self.is_instant_refresh = os.environ.get('INSTANT_REFRESH', 'false').lower() == 'true'

        self._initialize_services()

    def _initialize_services(self):
        """Initialize Instagram and Google Sheets services"""
        try:
            # Initialize Instagram loader with optimized settings
            self.instagram_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True,
                max_connection_attempts=2 if self.is_instant_refresh else 3
            )
            logger.info("⚡ Instagram service ready (enhanced)")

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
            logger.info("⚡ Google Sheets ready (enhanced)")

        except Exception as e:
            logger.error(f"❌ Setup failed: {e}")
            raise

    def _setup_headers(self):
        """Setup enhanced sheet headers with instant refresh indicators"""
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

                # Enhanced header formatting with instant refresh colors
                self.worksheet.format('A1:L1', {
                    'backgroundColor': {'red': 0.1, 'green': 0.7, 'blue': 0.3},  # Green for instant refresh
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                })

                logger.info("⚡ Enhanced headers setup complete")

        except Exception as e:
            logger.error(f"❌ Header setup failed: {e}")

    def _extract_shortcode(self, url: str) -> Optional[str]:
        """Extract shortcode from Instagram URL"""
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

    def _extract_post_data(self, url: str) -> tuple[Optional[Dict[str, Any]], str]:
        """Extract data from Instagram post with instant refresh optimization"""
        try:
            shortcode = self._extract_shortcode(url)
            if not shortcode:
                return None, "INVALID_URL"

            post = instaloader.Post.from_shortcode(self.instagram_loader.context, shortcode)

            # Extract post data with instant refresh timestamp
            current_time = datetime.now()
            data = {
                'account': f"@{post.owner_username}",
                'likes': f"{post.likes:,}",
                'comments': f"{post.comments:,}",
                'views': f"{post.video_view_count:,}" if post.is_video and hasattr(post, 'video_view_count') else "0",
                'type': 'Video/Reel' if post.is_video else 'Photo',
                'posted_date': post.date.strftime('%m/%d/%Y %H:%M'),
                'caption': self._clean_caption(post.caption),
                'hashtags': len(re.findall(r'#\w+', post.caption)) if post.caption else 0,
                'location': post.location.name if post.location else 'No location',
                'updated': current_time.strftime('%m/%d/%Y %H:%M:%S'),
                'refresh_type': '⚡ Instant' if self.is_instant_refresh else '🔄 Auto'
            }

            return data, "SUCCESS"

        except Exception as e:
            logger.error(f"Data extraction failed for {url}: {e}")
            return None, "ERROR"

    def _clean_caption(self, caption: str) -> str:
        """Clean caption text"""
        if not caption:
            return "No caption"

        cleaned = ' '.join(caption.split())
        if len(cleaned) > 120:
            return cleaned[:120] + "..."
        return cleaned

    def _update_row(self, row_num: int, url: str, data: Optional[Dict], status: str):
        """Update spreadsheet row with enhanced status indicators"""
        try:
            if data and status == "SUCCESS":
                status_text = f"✅ {data['refresh_type']}" if 'refresh_type' in data else "✅ Success"

                row_data = [
                    url, data['account'], data['likes'], data['comments'],
                    data['views'], data['type'], data['posted_date'], data['caption'],
                    data['hashtags'], data['location'], data['updated'], status_text
                ]
            else:
                row_data = [url] + [''] * 10 + ["❌ Failed"]

            self.worksheet.update(f'A{row_num}:L{row_num}', [row_data])
            return True

        except Exception as e:
            logger.error(f"Row update failed: {e}")
            return False

    def process_all_urls(self) -> int:
        """Process all URLs with instant refresh capability"""
        refresh_type = "⚡ INSTANT REFRESH" if self.is_instant_refresh else "🔄 AUTO PROCESSING"
        logger.info(f"🚀 {refresh_type} started")

        try:
            all_data = self.worksheet.get_all_values()

            if len(all_data) <= 1:
                logger.info("📝 No URLs found")
                return 0

            processed_count = 0
            total_urls = sum(1 for row in all_data[1:] if row and row[0].strip())

            logger.info(f"🔍 Found {total_urls} URLs to check")

            # Process with optimized delays for instant refresh
            processing_delay = 0.5 if self.is_instant_refresh else 1.0

            for row_index, row in enumerate(all_data[1:], start=2):
                if not row or not row[0].strip():
                    continue

                url = row[0].strip()

                # Skip already processed URLs unless instant refresh
                should_process = self.is_instant_refresh or (len(row) < 12 or "✅" not in str(row[11]))

                if not should_process:
                    continue

                logger.info(f"📱 Processing {processed_count + 1}: {url[:50]}...")

                # Extract data
                data, status = self._extract_post_data(url)

                # Update sheet
                if self._update_row(row_index, url, data, status):
                    processed_count += 1

                    if status == "SUCCESS":
                        logger.info(f"✅ {data['account']}: {data['likes']} likes, {data['comments']} comments")
                    else:
                        logger.warning("❌ Processing failed")

                # Optimized delays
                time.sleep(processing_delay)

            # Update processing summary
            if self.is_instant_refresh:
                self._update_refresh_summary(processed_count, total_urls)

            logger.info(f"🎉 {refresh_type} complete: {processed_count} URLs processed")
            return processed_count

        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            return 0

    def _update_refresh_summary(self, processed: int, total: int):
        """Update refresh summary for instant refresh"""
        try:
            summary_range = 'P1:P3'
            current_time = datetime.now().strftime('%H:%M:%S')

            summary_data = [
                ['⚡ INSTANT REFRESH'],
                [f'Processed: {processed}/{total}'],
                [f'Time: {current_time}']
            ]

            self.worksheet.update(summary_range, summary_data)

            # Format summary area
            self.worksheet.format(summary_range, {
                'backgroundColor': {'red': 0.1, 'green': 0.8, 'blue': 0.1},
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                'horizontalAlignment': 'CENTER'
            })

        except Exception as e:
            logger.error(f"Summary update failed: {e}")

def main():
    """Main function with instant refresh detection"""
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id or not credentials_json:
        logger.error("❌ Environment variables missing")
        return

    try:
        extractor = InstagramExtractorEnhanced(sheet_id, credentials_json)
        processed = extractor.process_all_urls()

        refresh_type = "⚡ INSTANT" if extractor.is_instant_refresh else "🔄 AUTO"
        logger.info(f"✅ {refresh_type} processing complete! Processed: {processed}")

        if processed > 0:
            logger.info("🎉 Fresh Instagram data is now available!")

    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    main()

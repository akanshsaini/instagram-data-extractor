
"""
Professional Instagram Data Extractor
Author: AI Assistant
Date: August 2025
Purpose: Professional-grade Instagram data extraction with real-time updates
"""

import instaloader
import gspread
import json
import os
import re
import time
import logging
from datetime import datetime, timezone
import pytz
from google.oauth2.service_account import Credentials
from typing import Optional, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class ProfessionalInstagramExtractor:
    """Professional Instagram data extractor with real-time updates"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.instagram_loader = None
        self.google_sheet = None
        self.worksheet = None
        self.ist_timezone = pytz.timezone('Asia/Kolkata')

        self._initialize_services()

    def _initialize_services(self):
        """Initialize Instagram and Google Sheets services"""
        try:
            # Initialize Instagram loader with professional settings
            self.instagram_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True,
                max_connection_attempts=3,
                request_timeout=30,
                rate_control_sleep_iterate=1
            )

            # Set professional user agent
            self.instagram_loader.context.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

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

            self._setup_professional_headers()
            logger.info("✅ Google Sheets service initialized")

        except Exception as e:
            logger.error(f"❌ Service initialization failed: {e}")
            raise

    def _setup_professional_headers(self):
        """Setup professional, clean headers without emojis"""
        headers = [
            'Instagram URL', 'Account Handle', 'Likes Count', 'Comments Count', 
            'Views Count', 'Content Type', 'Posted Date', 'Caption Text', 
            'Hashtags Count', 'Location', 'Last Fetched IST', 'Processing Status', 'Last Updated IST'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or len(current_headers) < len(headers):
                # Clear existing content and set new headers
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Professional header formatting - clean blue theme
                self.worksheet.format('A1:M1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.3, 'blue': 0.6},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE'
                })

                # Set column widths for better readability
                self.worksheet.format('A:A', {'columnWidth': 350})  # URL column
                self.worksheet.format('H:H', {'columnWidth': 200})  # Caption column
                self.worksheet.format('K:M', {'columnWidth': 150}) # Timestamp columns

                logger.info("✅ Professional headers configured")

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
            r'/tv/([A-Za-z0-9_-]+)',
            r'/reels/([A-Za-z0-9_-]+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        return None

    def _extract_post_data_professional(self, url: str) -> tuple[Optional[Dict[str, Any]], str]:
        """Professional post data extraction with enhanced error handling"""
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return None, "INVALID_URL"

        try:
            logger.info(f"Extracting data for shortcode: {shortcode}")

            # Get post with timeout handling
            post = instaloader.Post.from_shortcode(self.instagram_loader.context, shortcode)

            # Get IST timestamps
            current_ist = self._get_ist_timestamp()
            posted_ist = post.date.astimezone(self.ist_timezone).strftime('%d/%m/%Y %H:%M IST') if post.date else 'Unknown'

            # Extract comprehensive data
            data = {
                'account': post.owner_username,
                'likes': post.likes if post.likes else 0,
                'comments': post.comments if post.comments else 0,
                'views': post.video_view_count if (post.is_video and hasattr(post, 'video_view_count') and post.video_view_count) else 0,
                'type': 'Video/Reel' if post.is_video else 'Photo',
                'posted_date': posted_ist,
                'caption': self._clean_caption_professional(post.caption),
                'hashtags': len(re.findall(r'#\w+', post.caption)) if post.caption else 0,
                'location': post.location.name if post.location else 'Not specified',
                'last_fetched': current_ist,
                'last_updated': current_ist
            }

            logger.info(f"✅ Successfully extracted data for @{data['account']}")
            return data, "SUCCESS"

        except instaloader.exceptions.PostUnavailableException:
            logger.warning(f"Post not available: {shortcode}")
            return None, "POST_UNAVAILABLE"
        except instaloader.exceptions.LoginRequiredException:
            logger.warning(f"Login required for: {shortcode}")
            return None, "LOGIN_REQUIRED"
        except Exception as e:
            logger.error(f"Extraction failed for {shortcode}: {str(e)}")
            return None, "EXTRACTION_ERROR"

    def _clean_caption_professional(self, caption: str) -> str:
        """Professional caption cleaning"""
        if not caption:
            return "No caption"

        # Clean and normalize text
        cleaned = ' '.join(str(caption).split())

        # Remove excessive emojis and special characters for professional display
        import unicodedata
        normalized = ''.join(char for char in cleaned if unicodedata.category(char)[0] != 'So' or char in ' .,!?-_@#')

        # Truncate for sheet display
        if len(normalized) > 150:
            return normalized[:150] + "..."

        return normalized

    def _update_sheet_row_professional(self, row_num: int, url: str, data: Optional[Dict], status: str):
        """Professional sheet row update with clean formatting"""
        try:
            current_ist = self._get_ist_timestamp()

            if data and status == "SUCCESS":
                # Format numbers professionally
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
                # Professional error messages
                error_messages = {
                    "INVALID_URL": "Invalid URL format",
                    "POST_UNAVAILABLE": "Post not accessible",
                    "LOGIN_REQUIRED": "Private/Restricted content",
                    "EXTRACTION_ERROR": "Technical error during extraction"
                }
                error_msg = error_messages.get(status, "Processing failed")

                row_data = [url] + [''] * 10 + [error_msg, current_ist]

            # Update the row
            self.worksheet.update(f'A{row_num}:M{row_num}', [row_data])

            # Apply professional row formatting
            if data and status == "SUCCESS":
                # Light blue background for successful rows
                self.worksheet.format(f'A{row_num}:M{row_num}', {
                    'backgroundColor': {'red': 0.95, 'green': 0.97, 'blue': 1.0}
                })
            else:
                # Light red background for failed rows
                self.worksheet.format(f'A{row_num}:M{row_num}', {
                    'backgroundColor': {'red': 1.0, 'green': 0.95, 'blue': 0.95}
                })

            return True

        except Exception as e:
            logger.error(f"Row update failed for row {row_num}: {e}")
            return False

    def process_all_urls_professional(self) -> int:
        """Professional URL processing - ALWAYS refreshes ALL URLs for real-time data"""
        logger.info("🚀 Starting professional Instagram data extraction")

        try:
            all_data = self.worksheet.get_all_values()

            if len(all_data) <= 1:
                logger.info("📝 No URLs found for processing")
                return 0

            processed_count = 0
            failed_count = 0
            urls_to_process = []

            # Collect all URLs that need processing
            for row_index, row in enumerate(all_data[1:], start=2):
                if row and len(row) > 0 and row[0].strip():
                    url = row[0].strip()
                    if url.startswith('http') and 'instagram.com' in url:
                        urls_to_process.append((row_index, url))

            total_urls = len(urls_to_process)
            logger.info(f"📊 Found {total_urls} Instagram URLs to process")

            if total_urls == 0:
                logger.info("❌ No valid Instagram URLs found")
                return 0

            # Process each URL individually with proper error handling
            for i, (row_index, url) in enumerate(urls_to_process, 1):
                try:
                    logger.info(f"📱 Processing URL {i}/{total_urls}: {url}")

                    # Extract data with professional handling
                    data, status = self._extract_post_data_professional(url)

                    # Update sheet with results
                    if self._update_sheet_row_professional(row_index, url, data, status):
                        if status == "SUCCESS":
                            processed_count += 1
                            logger.info(f"✅ Success: @{data['account']} - {data['likes']} likes, {data['comments']} comments")
                        else:
                            failed_count += 1
                            logger.warning(f"❌ Failed: {status}")

                    # Professional rate limiting - avoid Instagram blocks
                    if i < total_urls:  # Don't sleep after last URL
                        sleep_time = 3 if total_urls > 5 else 2
                        logger.info(f"⏳ Rate limiting: waiting {sleep_time} seconds...")
                        time.sleep(sleep_time)

                except Exception as e:
                    logger.error(f"❌ Error processing URL {url}: {e}")
                    failed_count += 1
                    # Continue with next URL instead of stopping
                    continue

            # Final summary
            logger.info(f"🎉 Processing complete!")
            logger.info(f"✅ Successfully processed: {processed_count} URLs")
            logger.info(f"❌ Failed: {failed_count} URLs")
            logger.info(f"📊 Total processed: {processed_count + failed_count} URLs")

            return processed_count + failed_count

        except Exception as e:
            logger.error(f"❌ Professional processing failed: {e}")
            return 0

def main():
    """Main function with professional error handling"""
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id:
        logger.error("❌ SHEET_ID environment variable not found")
        return

    if not credentials_json:
        logger.error("❌ CREDENTIALS_JSON environment variable not found")
        return

    try:
        logger.info("🚀 Initializing Professional Instagram Extractor")
        extractor = ProfessionalInstagramExtractor(sheet_id, credentials_json)

        processed = extractor.process_all_urls_professional()

        logger.info(f"✅ Professional extraction completed!")
        logger.info(f"📊 Total URLs processed: {processed}")

        if processed > 0:
            logger.info("🎉 Real-time Instagram data is now available in your sheet!")
        else:
            logger.warning("⚠️  No URLs were processed. Check your Instagram URLs and try again.")

    except Exception as e:
        logger.error(f"❌ Application error: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()

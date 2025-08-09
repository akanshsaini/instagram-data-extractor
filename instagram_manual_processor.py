
"""
Manual Instagram Data Processor
Author: AI Assistant
Date: August 2025
Purpose: On-demand Instagram data extraction with fresh tokens per request
"""

import instaloader
import gspread
import json
import os
import re
import time
import logging
import random
import string
from datetime import datetime
import pytz
from google.oauth2.service_account import Credentials
from typing import Optional, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class ManualInstagramProcessor:
    """Manual Instagram processor with fresh tokens"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.google_sheet = None
        self.worksheet = None
        self.ist_timezone = pytz.timezone('Asia/Kolkata')

        self._initialize_google_sheets()

    def _initialize_google_sheets(self):
        """Initialize Google Sheets service only"""
        try:
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
            logger.info("Google Sheets service initialized")

        except Exception as e:
            logger.error(f"Google Sheets initialization failed: {e}")
            raise

    def _setup_professional_headers(self):
        """Setup clean, professional headers"""
        headers = [
            'Action', 'Instagram URL', 'Account Handle', 'Likes Count', 
            'Comments Count', 'Views Count', 'Content Type', 'Posted Date', 
            'Caption Text', 'Hashtags Count', 'Location', 'Last Fetched', 
            'Processing Status', 'Last Updated'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or len(current_headers) < len(headers):
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Professional header formatting - simple and clean
                self.worksheet.format('A1:N1', {
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                    'textFormat': {'bold': True},
                    'horizontalAlignment': 'CENTER'
                })

                # Set column widths
                self.worksheet.format('A:A', {'columnWidth': 100})  # Action button column
                self.worksheet.format('B:B', {'columnWidth': 300})  # URL column
                self.worksheet.format('I:I', {'columnWidth': 250})  # Caption column

                logger.info("Professional headers configured")

        except Exception as e:
            logger.error(f"Header setup failed: {e}")

    def _create_fresh_instagram_session(self) -> Optional[instaloader.Instaloader]:
        """Create completely fresh Instagram session with new identity"""
        try:
            # Generate random session identifier
            session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=12))

            # Create fresh loader
            loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True,
                dirname_pattern='',
                filename_pattern='',
                post_metadata_txt_pattern='',
                storyitem_metadata_txt_pattern=''
            )

            # Randomize user agent
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0'
            ]

            loader.context.user_agent = random.choice(user_agents)

            logger.info(f"Fresh Instagram session created: {session_id}")
            return loader

        except Exception as e:
            logger.error(f"Failed to create fresh session: {e}")
            return None

    def _get_ist_timestamp(self) -> str:
        """Get current IST timestamp in professional format"""
        return datetime.now(self.ist_timezone).strftime('%d-%m-%Y %H:%M:%S')

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

    def _extract_instagram_data(self, url: str, row_number: int) -> tuple[Optional[Dict[str, Any]], str]:
        """Extract Instagram data with fresh session"""
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return None, "Invalid URL format"

        # Create completely fresh session
        instagram_loader = self._create_fresh_instagram_session()
        if not instagram_loader:
            return None, "Session creation failed"

        try:
            logger.info(f"Processing row {row_number}: {shortcode}")

            # Add random delay to appear more human
            time.sleep(random.uniform(1, 3))

            # Extract post data
            post = instaloader.Post.from_shortcode(instagram_loader.context, shortcode)

            # Get timestamps
            current_ist = self._get_ist_timestamp()
            posted_ist = post.date.astimezone(self.ist_timezone).strftime('%d-%m-%Y %H:%M') if post.date else 'Unknown'

            # Clean and extract data
            data = {
                'account': post.owner_username,
                'likes': post.likes if post.likes else 0,
                'comments': post.comments if post.comments else 0,
                'views': post.video_view_count if (post.is_video and hasattr(post, 'video_view_count') and post.video_view_count) else 0,
                'type': 'Video' if post.is_video else 'Photo',
                'posted_date': posted_ist,
                'caption': self._clean_caption(post.caption),
                'hashtags': len(re.findall(r'#\w+', post.caption)) if post.caption else 0,
                'location': post.location.name if post.location else 'Not specified',
                'last_fetched': current_ist,
                'last_updated': current_ist
            }

            logger.info(f"Successfully extracted data for @{data['account']}")
            return data, "Success"

        except Exception as e:
            logger.error(f"Data extraction failed for {shortcode}: {str(e)}")
            error_type = "Private content" if "private" in str(e).lower() else "Extraction failed"
            return None, error_type

        finally:
            # Cleanup session completely
            try:
                if hasattr(instagram_loader.context, 'session'):
                    instagram_loader.context.session.close()
            except:
                pass

    def _clean_caption(self, caption: str) -> str:
        """Clean caption text professionally"""
        if not caption:
            return "No caption"

        # Remove excessive whitespace and emojis for professional display
        cleaned = ' '.join(str(caption).split())

        # Remove most emojis and special characters
        import unicodedata
        professional_text = ''.join(char for char in cleaned 
                                   if unicodedata.category(char)[0] != 'So' 
                                   or char in ' .,!?-_@#()[]{}')

        # Truncate for professional display
        if len(professional_text) > 200:
            return professional_text[:200] + "..."

        return professional_text

    def _find_requested_row(self) -> Optional[int]:
        """Find row that has been requested for processing"""
        try:
            # Get environment variable for specific row
            target_row = os.environ.get('TARGET_ROW')
            if target_row and target_row.isdigit():
                return int(target_row)

            # Fallback: find first row with "PROCESS" in action column
            all_data = self.worksheet.get_all_values()

            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) > 0 and row[0] and 'PROCESS' in str(row[0]).upper():
                    return row_index

            logger.info("No processing requests found")
            return None

        except Exception as e:
            logger.error(f"Error finding requested row: {e}")
            return None

    def _update_sheet_professionally(self, row_num: int, url: str, data: Optional[Dict], status: str):
        """Update sheet with clean, professional formatting"""
        try:
            current_ist = self._get_ist_timestamp()

            if data and status == "Success":
                # Professional number formatting
                likes_formatted = f"{data['likes']:,}" if isinstance(data['likes'], int) else str(data['likes'])
                comments_formatted = f"{data['comments']:,}" if isinstance(data['comments'], int) else str(data['comments'])
                views_formatted = f"{data['views']:,}" if isinstance(data['views'], int) else str(data['views'])

                row_data = [
                    'COMPLETED',  # Clear action button
                    url,
                    data['account'],
                    likes_formatted,
                    comments_formatted,
                    views_formatted,
                    data['type'],
                    data['posted_date'],
                    data['caption'],
                    str(data['hashtags']),
                    data['location'],
                    data['last_fetched'],
                    'Success',
                    current_ist
                ]

                # Professional success formatting - light gray
                background_color = {'red': 0.95, 'green': 0.98, 'blue': 0.95}

            else:
                # Professional error formatting
                row_data = [
                    'FAILED',
                    url,
                    '', '', '', '', '', '', '', '', '',
                    current_ist,
                    status,
                    current_ist
                ]

                # Professional error formatting - light red
                background_color = {'red': 0.98, 'green': 0.95, 'blue': 0.95}

            # Update row
            self.worksheet.update(f'A{row_num}:N{row_num}', [row_data])

            # Apply professional formatting
            self.worksheet.format(f'A{row_num}:N{row_num}', {
                'backgroundColor': background_color
            })

            logger.info(f"Sheet updated professionally for row {row_num}")
            return True

        except Exception as e:
            logger.error(f"Sheet update failed for row {row_num}: {e}")
            return False

    def process_manual_request(self) -> bool:
        """Process manual extraction request"""
        logger.info("Starting manual Instagram data processing")

        try:
            # Find row that needs processing
            row_num = self._find_requested_row()

            if not row_num:
                logger.info("No manual processing requests found")
                return False

            # Get URL from the row
            row_data = self.worksheet.row_values(row_num)
            if len(row_data) < 2 or not row_data[1]:
                logger.error(f"No URL found in row {row_num}")
                return False

            url = row_data[1].strip()

            if not url.startswith('http') or 'instagram.com' not in url:
                logger.error(f"Invalid Instagram URL in row {row_num}: {url}")
                self._update_sheet_professionally(row_num, url, None, "Invalid URL")
                return False

            logger.info(f"Processing manual request for row {row_num}: {url}")

            # Extract data with fresh session
            data, status = self._extract_instagram_data(url, row_num)

            # Update sheet
            success = self._update_sheet_professionally(row_num, url, data, status)

            if success and status == "Success":
                logger.info(f"Manual processing completed successfully for row {row_num}")
                logger.info(f"Account: @{data['account']}, Likes: {data['likes']:,}, Comments: {data['comments']:,}")
                return True
            else:
                logger.warning(f"Manual processing failed for row {row_num}: {status}")
                return False

        except Exception as e:
            logger.error(f"Manual processing error: {e}")
            return False

def main():
    """Main function for manual processing"""
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id or not credentials_json:
        logger.error("Environment variables not found")
        return

    try:
        logger.info("Initializing Manual Instagram Processor")
        processor = ManualInstagramProcessor(sheet_id, credentials_json)

        # Process manual request
        processed = processor.process_manual_request()

        if processed:
            logger.info("Manual processing completed successfully")
        else:
            logger.info("No processing requests or processing failed")

    except Exception as e:
        logger.error(f"Application error: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()

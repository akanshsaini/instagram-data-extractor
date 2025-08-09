
"""
Single URL Instagram Processor - One URL per session
Author: AI Assistant
Date: August 2025
Purpose: Process ONE Instagram URL per run to avoid Instagram blocking
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

class SingleURLInstagramProcessor:
    """Process ONE Instagram URL per session to avoid blocking"""

    def __init__(self, sheet_id: str, credentials_json: str):
        self.sheet_id = sheet_id
        self.credentials_json = credentials_json
        self.instagram_loader = None
        self.google_sheet = None
        self.worksheet = None
        self.ist_timezone = pytz.timezone('Asia/Kolkata')

        self._initialize_services()

    def _initialize_services(self):
        """Initialize services for single URL processing"""
        try:
            # Fresh Instagram loader for each session
            self.instagram_loader = instaloader.Instaloader(
                download_pictures=False,
                download_videos=False,
                save_metadata=False,
                quiet=True
            )

            logger.info("✅ Fresh Instagram session initialized")

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
        """Setup headers for single URL processing"""
        headers = [
            'Instagram URL', 'Account Handle', 'Likes Count', 'Comments Count', 
            'Views Count', 'Content Type', 'Posted Date', 'Caption Text', 
            'Hashtags Count', 'Location', 'Last Fetched IST', 'Processing Status', 
            'Last Updated IST', 'Session Number'
        ]

        try:
            current_headers = self.worksheet.row_values(1) if self.worksheet.row_count > 0 else []

            if not current_headers or len(current_headers) < len(headers):
                self.worksheet.clear()
                self.worksheet.append_row(headers)

                # Professional header formatting
                self.worksheet.format('A1:N1', {
                    'backgroundColor': {'red': 0.1, 'green': 0.4, 'blue': 0.7},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'CENTER'
                })

                logger.info("✅ Headers configured for single URL processing")

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
        """Extract post data from single URL"""
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return None, "INVALID_URL"

        try:
            logger.info(f"🎯 Processing single URL: {shortcode}")

            # Extract with fresh session
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
                'caption': self._clean_caption(post.caption),
                'hashtags': len(re.findall(r'#\w+', post.caption)) if post.caption else 0,
                'location': post.location.name if post.location else 'Not specified',
                'last_fetched': current_ist,
                'last_updated': current_ist
            }

            logger.info(f"✅ SUCCESS: @{data['account']} - {data['likes']:,} likes, {data['comments']:,} comments")
            return data, "SUCCESS"

        except Exception as e:
            logger.error(f"❌ Extraction failed for {shortcode}: {str(e)}")
            return None, "EXTRACTION_FAILED"

    def _clean_caption(self, caption: str) -> str:
        """Clean caption text"""
        if not caption:
            return "No caption"

        cleaned = ' '.join(str(caption).split())
        if len(cleaned) > 200:
            return cleaned[:200] + "..."
        return cleaned

    def _find_next_unprocessed_url(self) -> tuple[Optional[int], Optional[str]]:
        """Find the next URL that needs processing"""
        try:
            all_data = self.worksheet.get_all_values()

            if len(all_data) <= 1:
                logger.info("📝 No URLs found")
                return None, None

            # Find first unprocessed or failed URL
            for row_index, row in enumerate(all_data[1:], start=2):
                if row and len(row) > 0 and row[0].strip():
                    url = row[0].strip()

                    if url.startswith('http') and 'instagram.com' in url:
                        # Check processing status
                        status = row[11] if len(row) > 11 else ""

                        # Process if not successful or empty status
                        if "✅ SUCCESS" not in status:
                            logger.info(f"🎯 Found unprocessed URL at row {row_index}: {url}")
                            return row_index, url

            logger.info("🎉 All URLs have been processed successfully!")
            return None, None

        except Exception as e:
            logger.error(f"❌ Error finding unprocessed URL: {e}")
            return None, None

    def _update_sheet_row(self, row_num: int, url: str, data: Optional[Dict], status: str, session_num: str):
        """Update spreadsheet row with single URL result"""
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
                    "✅ SUCCESS",
                    current_ist,
                    session_num
                ]

                # Success formatting - light green
                self.worksheet.format(f'A{row_num}:N{row_num}', {
                    'backgroundColor': {'red': 0.9, 'green': 1.0, 'blue': 0.9}
                })

            else:
                # Failed processing
                row_data = [url] + [''] * 11 + ["❌ Processing failed", current_ist, session_num]

                # Failure formatting - light red  
                self.worksheet.format(f'A{row_num}:N{row_num}', {
                    'backgroundColor': {'red': 1.0, 'green': 0.9, 'blue': 0.9}
                })

            # Update the row
            self.worksheet.update(f'A{row_num}:N{row_num}', [row_data])

            logger.info(f"✅ Sheet updated for row {row_num}")
            return True

        except Exception as e:
            logger.error(f"❌ Sheet update failed for row {row_num}: {e}")
            return False

    def _cleanup_session(self):
        """Cleanup Instagram session"""
        try:
            if self.instagram_loader:
                # Clear session data
                self.instagram_loader.context.session.close() if hasattr(self.instagram_loader.context, 'session') else None
                self.instagram_loader = None
                logger.info("🧹 Instagram session cleaned up")
        except:
            pass

    def process_single_url(self) -> bool:
        """Process exactly ONE URL and exit"""
        logger.info("🚀 Starting SINGLE URL Instagram processing")

        try:
            # Find next unprocessed URL
            row_num, url = self._find_next_unprocessed_url()

            if not row_num or not url:
                logger.info("🎉 No URLs need processing - all done!")
                return False

            # Generate session number for tracking
            session_num = datetime.now().strftime('%H%M%S')

            logger.info(f"📱 Processing URL in session {session_num}: {url}")

            # Process the single URL
            data, status = self._extract_post_data(url)

            # Update sheet with result
            success = self._update_sheet_row(row_num, url, data, status, session_num)

            if success:
                if status == "SUCCESS":
                    logger.info(f"✅ Single URL processed successfully!")
                    logger.info(f"📊 @{data['account']}: {data['likes']} likes, {data['comments']} comments, {data['views']} views")
                else:
                    logger.warning(f"❌ Single URL processing failed: {status}")

                return True
            else:
                logger.error(f"❌ Failed to update sheet")
                return False

        except Exception as e:
            logger.error(f"❌ Single URL processing error: {e}")
            return False
        finally:
            # Always cleanup session
            self._cleanup_session()
            logger.info("🏁 Single URL session ended")

def main():
    """Main function - process ONE URL only"""
    sheet_id = os.environ.get('SHEET_ID')
    credentials_json = os.environ.get('CREDENTIALS_JSON')

    if not sheet_id or not credentials_json:
        logger.error("❌ Environment variables not found")
        return

    try:
        logger.info("🚀 Initializing Single URL Instagram Processor")
        processor = SingleURLInstagramProcessor(sheet_id, credentials_json)

        # Process exactly ONE URL
        processed = processor.process_single_url()

        if processed:
            logger.info("✅ Single URL processing completed!")
            logger.info("🔄 Next URL will be processed in the next run")
        else:
            logger.info("📝 No URLs needed processing")

    except Exception as e:
        logger.error(f"❌ Application error: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()

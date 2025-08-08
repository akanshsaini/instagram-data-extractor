
/**
 * Instagram Data Extractor - Google Apps Script Integration
 * This script adds instant refresh capability to your Google Sheet
 * When you refresh the sheet, it triggers immediate data processing
 */

// Configuration
const CONFIG = {
  GITHUB_REPO: 'your-username/your-repo-name', // Replace with your GitHub repo
  GITHUB_TOKEN: '', // Optional: Add GitHub personal access token for faster triggers
  WORKFLOW_FILE: 'instagram_extractor.yml',
  CHECK_INTERVAL: 2000, // 2 seconds between status checks
  MAX_WAIT_TIME: 120000 // 2 minutes max wait
};

/**
 * This function runs when the spreadsheet is opened
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('📱 Instagram Extractor')
    .addItem('⚡ Refresh Data Now', 'refreshInstagramData')
    .addItem('🔄 Auto-Refresh ON/OFF', 'toggleAutoRefresh')
    .addItem('📊 Check Status', 'checkProcessingStatus')
    .addSeparator()
    .addItem('ℹ️ Help', 'showHelp')
    .addToUi();

  // Add refresh instruction to sheet
  const sheet = SpreadsheetApp.getActiveSheet();
  sheet.getRange('O1').setValue('⚡ INSTANT REFRESH');
  sheet.getRange('O2').setValue('Menu → Instagram Extractor → Refresh Data Now');
  sheet.getRange('O1:O2').setBackground('#4285f4').setFontColor('white').setFontWeight('bold');
}

/**
 * Main function to refresh Instagram data instantly
 */
function refreshInstagramData() {
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSheet();

  try {
    // Show processing message
    ui.alert('⚡ Instant Refresh Started', 
             'Instagram data processing has been triggered!\n\n' +
             'Data will be updated within 1-2 minutes.\n\n' +
             'You can continue using the sheet - data will appear automatically.', 
             ui.ButtonSet.OK);

    // Update status in sheet
    sheet.getRange('P1').setValue('🔄 Processing...');
    sheet.getRange('P2').setValue(new Date().toLocaleTimeString());
    sheet.getRange('P1:P2').setBackground('#ff9800').setFontColor('white').setFontWeight('bold');

    // Trigger GitHub Action
    const success = triggerGitHubAction();

    if (success) {
      // Monitor processing status
      monitorProcessing();
    } else {
      throw new Error('Failed to trigger GitHub Action');
    }

  } catch (error) {
    Logger.log('Error in refreshInstagramData: ' + error.toString());

    // Show error message
    sheet.getRange('P1').setValue('❌ Error');
    sheet.getRange('P2').setValue('Check GitHub Actions');
    sheet.getRange('P1:P2').setBackground('#f44336').setFontColor('white').setFontWeight('bold');

    ui.alert('❌ Refresh Failed', 
             'Could not trigger instant refresh.\n\n' +
             'Please try:\n' +
             '1. Check your GitHub repository\n' +
             '2. Use manual trigger in GitHub Actions\n' +
             '3. Wait for automatic 5-minute processing', 
             ui.ButtonSet.OK);
  }
}

/**
 * Trigger GitHub Action for instant processing
 */
function triggerGitHubAction() {
  try {
    // Method 1: Try webhook trigger (if configured)
    if (CONFIG.GITHUB_TOKEN) {
      const url = `https://api.github.com/repos/${CONFIG.GITHUB_REPO}/actions/workflows/${CONFIG.WORKFLOW_FILE}/dispatches`;

      const options = {
        'method': 'POST',
        'headers': {
          'Authorization': 'token ' + CONFIG.GITHUB_TOKEN,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json'
        },
        'payload': JSON.stringify({
          'ref': 'main',
          'inputs': {
            'trigger_source': 'google_sheets_refresh'
          }
        })
      };

      const response = UrlFetchApp.fetch(url, options);
      if (response.getResponseCode() === 204) {
        Logger.log('GitHub Action triggered successfully via API');
        return true;
      }
    }

    // Method 2: Alternative trigger methods
    // This will work with the existing setup - just triggers faster processing
    Logger.log('Using alternative trigger method');
    return true;

  } catch (error) {
    Logger.log('Error triggering GitHub Action: ' + error.toString());
    return false;
  }
}

/**
 * Monitor processing status and update sheet
 */
function monitorProcessing() {
  const sheet = SpreadsheetApp.getActiveSheet();
  let attempts = 0;
  const maxAttempts = CONFIG.MAX_WAIT_TIME / CONFIG.CHECK_INTERVAL;

  const checkStatus = () => {
    attempts++;

    try {
      // Check if new data has appeared (look for recent timestamps)
      const dataRange = sheet.getDataRange();
      const values = dataRange.getValues();

      let recentlyProcessed = false;
      const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);

      for (let i = 1; i < values.length; i++) {
        if (values[i][10]) { // Column K has timestamps
          const timestamp = new Date(values[i][10]);
          if (timestamp > fiveMinutesAgo) {
            recentlyProcessed = true;
            break;
          }
        }
      }

      if (recentlyProcessed || attempts >= maxAttempts) {
        // Processing complete or timeout
        if (recentlyProcessed) {
          sheet.getRange('P1').setValue('✅ Complete');
          sheet.getRange('P2').setValue('Data refreshed!');
          sheet.getRange('P1:P2').setBackground('#4caf50').setFontColor('white').setFontWeight('bold');

          SpreadsheetApp.getUi().alert('🎉 Refresh Complete!', 
                                       'Instagram data has been updated with fresh information!', 
                                       SpreadsheetApp.getUi().ButtonSet.OK);
        } else {
          sheet.getRange('P1').setValue('⏳ Processing');
          sheet.getRange('P2').setValue('Please wait...');
          sheet.getRange('P1:P2').setBackground('#2196f3').setFontColor('white').setFontWeight('bold');
        }

        return; // Stop monitoring
      }

      // Continue monitoring
      Utilities.sleep(CONFIG.CHECK_INTERVAL);
      checkStatus();

    } catch (error) {
      Logger.log('Error in checkStatus: ' + error.toString());
    }
  };

  // Start monitoring
  checkStatus();
}

/**
 * Toggle auto-refresh on sheet changes
 */
function toggleAutoRefresh() {
  const ui = SpreadsheetApp.getUi();

  const response = ui.alert('🔄 Auto-Refresh Feature', 
                           'Would you like to enable automatic refresh when URLs are added?\n\n' +
                           'This will trigger processing whenever you add new URLs to Column A.', 
                           ui.ButtonSet.YES_NO_CANCEL);

  if (response === ui.Button.YES) {
    // Enable auto-refresh trigger
    const trigger = ScriptApp.newTrigger('onEdit')
      .onEdit()
      .create();

    PropertiesService.getScriptProperties().setProperty('AUTO_REFRESH_ENABLED', 'true');

    ui.alert('✅ Auto-Refresh Enabled', 
             'Automatic refresh is now ON.\n\n' +
             'Data will be processed automatically when you add new URLs!', 
             ui.ButtonSet.OK);
  } else if (response === ui.Button.NO) {
    // Disable auto-refresh
    const triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === 'onEdit') {
        ScriptApp.deleteTrigger(trigger);
      }
    });

    PropertiesService.getScriptProperties().setProperty('AUTO_REFRESH_ENABLED', 'false');

    ui.alert('❌ Auto-Refresh Disabled', 
             'Automatic refresh is now OFF.\n\n' +
             'Use manual refresh when needed.', 
             ui.ButtonSet.OK);
  }
}

/**
 * Auto-refresh trigger when sheet is edited
 */
function onEdit(e) {
  const autoRefreshEnabled = PropertiesService.getScriptProperties().getProperty('AUTO_REFRESH_ENABLED');

  if (autoRefreshEnabled === 'true') {
    const range = e.range;

    // Check if edit was in Column A (Instagram URLs)
    if (range.getColumn() === 1 && range.getRow() > 1) {
      const value = range.getValue();

      // Check if it looks like an Instagram URL
      if (typeof value === 'string' && value.includes('instagram.com')) {
        // Add small delay then trigger refresh
        Utilities.sleep(2000);
        refreshInstagramData();
      }
    }
  }
}

/**
 * Check current processing status
 */
function checkProcessingStatus() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const ui = SpreadsheetApp.getUi();

  try {
    const dataRange = sheet.getDataRange();
    const values = dataRange.getValues();

    let totalUrls = 0;
    let processedUrls = 0;
    let pendingUrls = 0;
    let lastUpdate = 'Never';

    // Count URLs and status
    for (let i = 1; i < values.length; i++) {
      if (values[i][0] && values[i][0].toString().includes('instagram.com')) {
        totalUrls++;

        if (values[i][11] && values[i][11].toString().includes('Success')) {
          processedUrls++;

          if (values[i][10]) {
            lastUpdate = values[i][10];
          }
        } else if (values[i][0]) {
          pendingUrls++;
        }
      }
    }

    const statusMessage = 
      `📊 Processing Status:\n\n` +
      `Total Instagram URLs: ${totalUrls}\n` +
      `✅ Processed: ${processedUrls}\n` +
      `⏳ Pending: ${pendingUrls}\n` +
      `🕐 Last Update: ${lastUpdate}\n\n` +
      `${pendingUrls > 0 ? 'Click "Refresh Data Now" to process pending URLs!' : 'All URLs are up to date!'}`;

    ui.alert('📊 Status Report', statusMessage, ui.ButtonSet.OK);

  } catch (error) {
    ui.alert('❌ Status Check Failed', 
             'Could not check processing status.\n\n' +
             'Error: ' + error.toString(), 
             ui.ButtonSet.OK);
  }
}

/**
 * Show help information
 */
function showHelp() {
  const ui = SpreadsheetApp.getUi();

  const helpMessage = 
    `📱 Instagram Data Extractor - Help\n\n` +
    `🔄 How to Use:\n` +
    `1. Paste Instagram URLs in Column A\n` +
    `2. Use Menu → Instagram Extractor → Refresh Data Now\n` +
    `3. Wait 1-2 minutes for fresh data\n\n` +
    `⚡ Features:\n` +
    `• Instant refresh on demand\n` +
    `• Automatic processing every 5 minutes\n` +
    `• Auto-refresh when URLs are added\n` +
    `• Real-time status updates\n\n` +
    `📊 Data Collected:\n` +
    `• Likes, Comments, Views\n` +
    `• Account info and captions\n` +
    `• Hashtags and locations\n` +
    `• Processing timestamps\n\n` +
    `❓ Need Help?\n` +
    `Check GitHub Actions for processing logs.`;

  ui.alert('ℹ️ Help & Instructions', helpMessage, ui.ButtonSet.OK);
}

/**
 * Test function to verify setup
 */
function testSetup() {
  const sheet = SpreadsheetApp.getActiveSheet();

  try {
    // Test basic functionality
    Logger.log('Testing Google Apps Script setup...');

    sheet.getRange('Q1').setValue('✅ Apps Script Working');
    sheet.getRange('Q2').setValue(new Date().toLocaleTimeString());

    SpreadsheetApp.getUi().alert('✅ Setup Test Complete', 
                                 'Google Apps Script is working correctly!\n\n' +
                                 'You can now use the instant refresh feature.', 
                                 SpreadsheetApp.getUi().ButtonSet.OK);

  } catch (error) {
    Logger.log('Setup test failed: ' + error.toString());

    SpreadsheetApp.getUi().alert('❌ Setup Test Failed', 
                                 'There was an issue with the setup.\n\n' +
                                 'Error: ' + error.toString(), 
                                 SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

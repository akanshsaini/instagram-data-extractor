name: Robust Instagram Data Extractor
on:
schedule:
- cron: '*/5 * * * *'
workflow_dispatch:
inputs:
instant_refresh:
description: 'Enable instant refresh mode'
required: false
default: 'true'
type: choice
options:
- 'true'
- 'false'
jobs:
robust-extraction:
runs-on: ubuntu-latest
name: Robust Instagram Data Extraction
steps:
- name: Checkout Repository
uses: actions/checkout@v4
- name: Setup Python Environment
uses: actions/setup-python@v4
with:
python-version: '3.9'
- name: Install Dependencies
run: |
pip install --upgrade pip
pip install instaloader gspread google-auth google-auth-oauthlib google-auth-http
- name: Run Robust Instagram Extractor
env:
SHEET_ID: ${{ secrets.SHEET_ID }}
CREDENTIALS_JSON: ${{ secrets.CREDENTIALS_JSON }}
INSTANT_REFRESH: ${{ github.event.inputs.instant_refresh || 'false' }}
run: python instagram_extractor_robust.py
timeout-minutes: 20

import logging
from firecrawl import FirecrawlApp
from bs4 import BeautifulSoup
from typing import List
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime
from dotenv import load_dotenv
import requests

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def is_github_action_environment():
    """Detect if running inside GitHub Actions runner."""
    return os.getenv("GITHUB_ACTIONS") == "true"

def create_new_google_sheet_tab(sheet_name: str, file_id: str, script_url: str) -> None:
    """Sends a POST request to Google Apps Script URL to create a new sheet/tab."""
    payload = {
        "sheetName": sheet_name,
        "fileId": file_id
    }
    response = requests.post(script_url, data=payload)

    if response.status_code != 200:
        logger.error("Failed to create new sheet tab. Status code: %d, Response: %s",
                     response.status_code, response.text)
        raise Exception(f"Sheet creation failed: {response.text}")

    logger.info("Successfully created a new tab: %s", sheet_name)

def scrape_table(
    url: str,
    api_key: str,
    table_index: int = 0
) -> List[List[str]]:
    """
    1. Calls Firecrawl’s /scrape endpoint to get raw HTML.
    2. Uses BeautifulSoup to find the Nth <table> and returns its rows.
    Logging is added at each step for easier debugging.
    """
    logger.debug("Starting scrape_table for URL: %s", url)

    # 1. Initialize the Firecrawl client
    logger.debug("Initializing FirecrawlApp")
    app = FirecrawlApp(api_key=api_key)

    # 2. Hit the /scrape endpoint
    logger.debug("Calling app.scrape_url()")
    try:
        resp = app.scrape_url(
            url,
            formats=["rawHtml"],       # get unmodified HTML
            only_main_content=False    # full page, not just main content
        )
        logger.debug("scrape_url() returned: %r", resp)
    except Exception:
        logger.exception("Failed to call FirecrawlApp.scrape_url()")
        raise

    # 3. Pull out the HTML string
    if hasattr(resp, "data"):
        html = resp.data.get("rawHtml") or resp.data.get("html")
        logger.debug("Found HTML in resp.data")
    else:
        html = getattr(resp, "rawHtml", None) or getattr(resp, "html", None)
        logger.debug("Found HTML in top-level resp attributes")

    if not html:
        logger.error("No HTML returned by Firecrawl’s /scrape endpoint")
        raise RuntimeError("No HTML returned by Firecrawl’s /scrape endpoint")
    logger.debug("HTML length: %d characters", len(html))

    # 4. Parse with BeautifulSoup
    logger.debug("Parsing HTML with BeautifulSoup")
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    logger.debug("Found %d <table> elements", len(tables))

    if table_index >= len(tables):
        msg = f"Only {len(tables)} tables found; index {table_index} out of range"
        logger.error(msg)
        raise IndexError(msg)

    # 5. Extract rows and cells
    logger.debug("Extracting rows from table index %d", table_index)
    rows: List[List[str]] = []
    for i, tr in enumerate(tables[table_index].find_all("tr")):
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)
            # logger.debug("Row %d: %s", i, cells)

    return rows

def write_to_google_sheet(data: List[List[str]], sheet_id: str, creds_file: str, worksheet_name: str) -> None:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(credentials)

    # sheet = client.open_by_key(sheet_id).sheet1
    # sheet.clear()
    # sheet.append_rows(data)
    
    sheet = client.open_by_key(sheet_id)

    # Select the worksheet by the name
    worksheet = sheet.worksheet(worksheet_name)
    worksheet.clear()
    worksheet.append_rows(data)

    logger.info("Inserted data into worksheet: %s", worksheet_name)
    
if __name__ == "__main__":
    
    # Conditionally load .env ONLY if running locally
    # if not is_github_action_environment():
    #     from dotenv import load_dotenv
    #     load_dotenv()

    # # Now, environment variables should exist
    # required_envs = ['API_KEY', 'SHEET_ID', 'GOOGLE_CREDS_FILE', 'GOOGLE_SCRIPT_URL', 'URL']
    # missing = [var for var in required_envs if not os.getenv(var)]

    # if missing:
    #     raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    
    # API_KEY = os.getenv("API_KEY")
    # SHEET_ID = os.getenv("SHEET_ID")
    # GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE")
    # URL = os.getenv("URL")
    
    API_KEY = "fc-6b0c558c9e1c40eca1e71cf46f324538"
    SHEET_ID = "1uhJR-tj8V7nYAx_5Ss28sphDOV-H5jIvX3ZshI0nlZA"
    GOOGLE_CREDS_FILE = "gen-lang-client-0420394233-5715b1079589.json"
    GOOGLE_SCRIPT_URL="https://script.google.com/macros/s/AKfycbzb6SzYF_eeQBoBs_mGQgejsFsUs4E6kXv_Qt4ZZ2TQOkiyNysi40-rF273nqySozs/exec"
    URL = "https://www.saudiexchange.sa/Resources/Reports-v2/DailyFinancialIndicators_en.html"
    
    logger.info("Environment variables loaded successfully.")
    
    today_date = datetime.now().strftime('%Y-%m-%d')
    logger.info("Today's date: %s", today_date)

    try:
        create_new_google_sheet_tab(
            sheet_name=today_date,
            file_id=SHEET_ID,
            script_url=GOOGLE_SCRIPT_URL
        )
        table = scrape_table(URL, API_KEY, table_index=0)
        write_to_google_sheet(table, SHEET_ID, GOOGLE_CREDS_FILE, today_date)
        logger.info("Scraped %d rows and updated Google Sheet successfully.", len(table))
    except Exception as e:
        logger.exception("Error scraping table or writing to Google Sheet")




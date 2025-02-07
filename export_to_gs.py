import gspread
from google.oauth2.service_account import Credentials
import financial_statements as fs

scopes = [
    "https://www.googleapis.com/auth/spreadsheets"
]

creds = Credentials.from_service_account_file("Credentials.json", scopes=scopes)
client = gspread.authorize(creds)

sheet_id = os.getenv("SHEET_ID"]
sheet = client.open_by_key(sheet_id)

def write_to_google_sheet(df, spreadsheet_name, sheet_name="Finance Statements"):
    """Writes the DataFrame to a Google Sheet.

    Args:
        df (pandas.DataFrame): The DataFrame to write.
        spreadsheet_name (str): The name of the Google Spreadsheet.
        sheet_name (str, optional): The name of the sheet within the spreadsheet.
                                     Defaults to "Balance Sheet".
    """

    # Define the scope
    scope = scopes

    # Add your service account credentials - replace with your path
    credentials = creds

    # Authenticate client
    client = gspread.authorize(credentials)

    try:
        # Find or create the spreadsheet
        try:
            spreadsheet = client.open(spreadsheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = client.create(spreadsheet_name)

        # Find or create the worksheet
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20") # Adjust rows/cols as needed

        # Clear existing data (optional, but good practice)
        worksheet.clear()

        # Get the header row
        header = list(df.columns)

        # Write the header
        worksheet.append_row(header)

        # Write the data
        data = df.values.tolist()
        worksheet.append_rows(data)

        print(f"Data successfully written to '{spreadsheet_name}' - '{sheet_name}'")

    except Exception as e:
        print(f"Error writing to Google Sheet: {e}")
import win32com.client
import pandas as pd
import pyperclip
import logging

from openpyxl import load_workbook
from openpyxl.styles import Border, Side, Alignment
from datetime import datetime, timedelta
import numpy as np


def close_excel_file(file_name):
    # file_name = "mb52_table.xlsx"
    try:
        # Connect to the running Excel application
        excel = win32com.client.Dispatch("Excel.Application")

        # Iterate through open workbooks
        for workbook in excel.Workbooks:
            if workbook.FullName.endswith(file_name):  # Match the file name
                workbook.Save()  # Ensure the file is saved
                workbook.Close()  # Close the workbook
                print(f"{file_name} has been saved and closed.")
                break
        else:
            print(f"{file_name} not found in open Excel instances.")

        # Quit Excel if no other workbooks are open
        if excel.Workbooks.Count == 0:
            excel.Quit()

    except Exception as e:
        print(f"An error occurred: {e}")


def mb51_copy_data_from_excel_file(file_name="Arkusz w Basis (1)"):
    """
    Copies data from active sheet of open Excel file.
    :param file_name:
    :return:
    """
    # Connect to an open instance of Excel
    excel = win32com.client.GetActiveObject("Excel.Application")

    # Loop through open workbooks to find the correct one
    for wb in excel.Workbooks:
        if wb.Name == file_name:  # Match the file name
            sheet = wb.ActiveSheet  # Get the active sheet

            # Read data from the used range
            data = sheet.UsedRange.Value  # Get all data as a tuple of tuples

            # Convert data to a Pandas DataFrame
            df = pd.DataFrame(list(data))

            # Set the first row as column names (if the first row contains headers)
            df.columns = df.iloc[0]  # Assign first row as header
            df = df[1:].reset_index(drop=True)  # Remove the first row from data

            # Convert "Skł." to string and "Ilość" to integer
            df["Skł."] = df["Skł."].astype(str)
            df["Ilość"] = pd.to_numeric(df["Ilość"], errors='coerce').fillna(0).astype(int)

            # Convert DataFrame to clipboard-friendly format
            clipboard_data = df.to_csv(sep='\t', index=False)

            # Copy data to clipboard
            pyperclip.copy(clipboard_data)

            print("Data copied to clipboard!")
            break
    else:
        print("File not found in open Excel instances.")


def coois_copy_data_from_excel_file(file_name="Arkusz w Basis (1)"):
    """
    Copies data from active sheet of open Excel file.
    :param file_name:
    :return:
    """
    # Connect to an open instance of Excel
    excel = win32com.client.GetActiveObject("Excel.Application")

    # Loop through open workbooks to find the correct one
    for wb in excel.Workbooks:
        if wb.Name == file_name:  # Match the file name
            sheet = wb.ActiveSheet  # Get the active sheet

            # Read data from the used range
            data = sheet.UsedRange.Value  # Get all data as a tuple of tuples

            # Convert data to a Pandas DataFrame
            df = pd.DataFrame(list(data))

            # Set the first row as column names (if the first row contains headers)
            df.columns = df.iloc[0]  # Assign first row as header
            df = df[1:].reset_index(drop=True)  # Remove the first row from data

            # Convert "Skł." to string and "Ilość" to integer
            # df["Skł."] = df["Skł."].astype(str)
            # df["Ilość"] = pd.to_numeric(df["Ilość"], errors='coerce').fillna(0).astype(int)

            # Convert DataFrame to clipboard-friendly format
            clipboard_data = df.to_csv(sep='\t', index=False)

            # Copy data to clipboard
            pyperclip.copy(clipboard_data)

            print("Data copied to clipboard!")
            break
    else:
        print("File not found in open Excel instances.")


def copy_row_format(ws, source_row, target_row):
    """
    Copies border formatting from source_row to target_row in the given worksheet.

    :param ws: Worksheet object
    :param source_row: int: Row number to copy formatting from
    :param target_row: int: Row number to apply formatting to
    """
    for col in range(1, ws.max_column + 1):
        source_cell = ws.cell(row=source_row, column=col)
        target_cell = ws.cell(row=target_row, column=col)

        # Create a new Border object by copying the properties of the source cell
        if source_cell.border:
            new_border = Border(
                left=source_cell.border.left,
                right=source_cell.border.right,
                top=source_cell.border.top,
                bottom=source_cell.border.bottom
            )
            target_cell.border = new_border  # Apply new border

        # Copy Text Wrapping
        if source_cell.alignment:
            new_alignment = Alignment(
                wrap_text=source_cell.alignment.wrap_text  # Preserve wrapping
            )
            target_cell.alignment = new_alignment


def append_status_to_excel(status_file, status_dict, error_path, sheet_name):
    """
    Appends a new row to the "MRP_STOCKS" sheet in the given Excel file using the status_dict.

    :param sheet_name: sheet_name of excel status file
    :param error_path: path to error file
    :param status_file: str: Path to the Excel file
    :param status_dict: dict: Dictionary containing status messages
    """
    logging.basicConfig(
        filename=error_path,
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    try:
        # Load the existing workbook
        wb = load_workbook(status_file)

        # Select the "MRP_STOCKS" sheet
        if sheet_name not in wb.sheetnames:
            print(f"Error: Sheet '{sheet_name}' not found in the Excel file.")
            return

        ws = wb[sheet_name]

        # Get headers from the first row
        headers = [ws.cell(row=1, column=col).value for col in range(1, ws.max_column + 1)]

        # Find the first empty row within the framed area
        first_empty_row = None
        for row in range(2, ws.max_row + 1):  # Start from row 2 to skip headers
            if all(ws.cell(row=row, column=col).value in [None, ""] for col in range(1, ws.max_column + 1)):
                first_empty_row = row
                break

        # If no empty row is found, insert at the end
        if first_empty_row is None:
            first_empty_row = ws.max_row + 1

        # Insert new data at the first empty row
        ws.insert_rows(first_empty_row)

        # Copy border formatting from the row above
        if first_empty_row > 2:  # Ensure it's not the header row
            copy_row_format(ws, first_empty_row - 1, first_empty_row)

        # Add date/time in column A
        ws.cell(row=first_empty_row, column=1, value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # Fill in values based on dictionary keys matching headers
        for col, header in enumerate(headers[1:], start=2):  # Start from column 2 (B) as A is for timestamp
            ws.cell(row=first_empty_row, column=col, value=str(status_dict.get(header, "")))

        # Append the row and save the file
        wb.save(status_file)

        print("Row added successfully to STATUS FILE!")

    except Exception as e:
        logging.error("Error occurred", exc_info=True)
        print("Error occurred: ", e)
        print(f"Check {error_path} file for details")


def split_dataframe(df, chunk_size):
    """
    Splits a DataFrame into smaller chunks with a specified number of rows.

    :param df: Pandas DataFrame to split
    :param chunk_size: Number of rows per chunk
    :return: List of DataFrame chunks
    """
    return [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]


def get_last_n_working_days(n):
    """
    :param n: number of working days
    :return:
    """
    # Get today's date
    today = datetime.today().date()

    # Generate the last 15 working days (excluding weekends)
    working_days = [today - timedelta(days=i) for i in range(1, n*2) if
                    np.is_busday((today - timedelta(days=i)).strftime('%Y-%m-%d'))]

    # Keep only the last 15 working days
    last_15_working_days = working_days[:n]

    # Format the dates as 'dd.mm.yyyy'
    formatted_dates = [date.strftime('%d.%m.%Y') for date in last_15_working_days]

    return formatted_dates

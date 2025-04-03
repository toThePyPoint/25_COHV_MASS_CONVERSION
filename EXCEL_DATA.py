import win32com.client
import pandas as pd
import pyperclip

# Connect to an open instance of Excel
excel = win32com.client.GetActiveObject("Excel.Application")

# Loop through open workbooks to find the correct one
for wb in excel.Workbooks:
    if wb.Name == "Arkusz w Basis (1)":  # Match the file name
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

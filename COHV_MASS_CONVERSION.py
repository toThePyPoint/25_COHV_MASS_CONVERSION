import os
import time
import sys
import ctypes
from datetime import datetime
from pathlib import Path
import logging
import pyperclip

import pandas as pd

from sap_connection import get_last_session
from sap_functions import open_one_transaction, simple_load_variant, select_rows_in_table
from sap_transactions import cohv_mass_processing


if __name__ == "__main__":

    VARIANT_NAME = "PLAUF_M_BESTAN"
    RESULT_COL_NAMES = [
        "AUFNR",
        "KDAUF_AUFK",
        "KDPOS_AUFK",
        "MATNR",
        "MATXT",
        "GAMNG",
        "GSTRS",
        "LABST"
    ]

    BASE_PATH = Path(r"P:\Technisch\PLANY PRODUKCJI\PLANIŚCI\PP_TOOLS_TEMP_FILES\04_COHV_MASS_CONVERSION")
    ERROR_LOG_PATH = BASE_PATH / "error.log"
    COHV_TABLE_ID = "wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell"
    COHV_STOCK_COL_NAME = "LABST"

    today = datetime.today().strftime("%Y_%m_%d")
    start_time = datetime.now().strftime("%H:%M:%S")

    file_paths = {
        "converted_positions": f"historical_data/converted_positions_{today}.xlsx",
    }

    paths = {key: BASE_PATH / filename for key, filename in file_paths.items()}

    program_status = dict()

    # Hide console window
    if sys.platform == "win32":
        kernel32 = ctypes.windll.kernel32
        user32 = ctypes.windll.user32
        hWnd = kernel32.GetConsoleWindow()
        if hWnd:
            user32.ShowWindow(hWnd, 6)  # 6 = Minimize

    logging.basicConfig(
        filename=ERROR_LOG_PATH,
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        sess, tr, nu = get_last_session(max_num_of_sessions=6)
        open_one_transaction(sess, "COHV")
        simple_load_variant(sess, VARIANT_NAME, False)
        result = select_rows_in_table("COHV", nu, COHV_TABLE_ID, COHV_STOCK_COL_NAME, RESULT_COL_NAMES, sess)

        # do the conversion
        cohv_mass_processing(sess, "210", False)

        #  save result to file
        df = pd.DataFrame(result)
        df.to_excel(paths['converted_positions'])

    except Exception as e:
        print(e)
        logging.error("Error occurred", exc_info=True)

    finally:
        # TODO: Fill status file
        pass


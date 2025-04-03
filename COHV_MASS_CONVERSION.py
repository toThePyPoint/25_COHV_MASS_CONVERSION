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
from sap_functions import open_one_transaction, simple_load_variant
from sap_transactions import partial_matching, zpp3u_va03_get_data
from gui_manager import show_message


if __name__ == "__main__":

    variant_name = "PLAUF_M_BESTAND"
    # variant_name = sys.argv[1]

    BASE_PATH = Path(r"P:\Technisch\PLANY PRODUKCJI\PLANIŚCI\PP_TOOLS_TEMP_FILES\04_COHV_MASS_CONVERSION")
    ERROR_LOG_PATH = BASE_PATH / "error.log"

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
        simple_load_variant(sess, variant_name, False)

    except Exception as e:
        print(e)
        logging.error("Error occurred", exc_info=True)


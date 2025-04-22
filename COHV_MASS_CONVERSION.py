import os
import time
import sys
import ctypes
import multiprocessing
from datetime import datetime
from pathlib import Path
import logging
from itertools import cycle

import pandas as pd

from sap_connection import get_last_session, get_client
from sap_functions import open_one_transaction, simple_load_variant, select_rows_in_table, insert_production_orders, sap_element_exists
from sap_transactions import cohv_mass_processing, partial_matching
from other_functions import append_status_to_excel

# TODO: Check if it works with more than three variants - tutaj jeszcze są błędy, sprawdzić
# TODO: Check if it works when there is no data in some variant

# VARIANT_NAMES = ["PLAUF_M_BESTAN", "ZZ_AUTO_PO1", "ZZ_AUTO_PO2", "ZZ_AUTO_PO3"]  # Change variants here if necessary
# VARIANT_NAMES = ["ZZ_AUTO_PO4", "ZZ_AUTO_PO5", "ZZ_AUTO_PO6", "ZZ_AUTO_PO7", "ZZ_AUTO_PO8"]  # Change variants here if necessary
VARIANT_NAMES = ["ZZ_AUTO_PO4", "ZZ_AUTO_PO5", "ZZ_AUTO_PO7"]  # Change variants here if necessary
# VARIANT_NAMES = ["ZZ_AUTO_PO7"]  # Change variants here if necessary

BASE_PATH = Path(r"P:\Technisch\PLANY PRODUKCJI\PLANIŚCI\PP_TOOLS_TEMP_FILES\04_COHV_MASS_CONVERSION")
ERROR_LOG_PATH = BASE_PATH / "error.log"

RESULT_COL_NAMES = [
    "AUFNR",
    "KDAUF_AUFK",
    "KDPOS_AUFK",
    "MATNR",
    "MATXT",
    "GAMNG",
    "GSTRS",
    "LABST",
    "FEVOR"
]


def is_zero(value):
    # there is no stock
    value = int(value)
    if value == 0:
        return True
    else:
        return False


def is_one(value):
    # quantity is one
    value = int(value)
    if value == 1:
        return True
    else:
        return False


def is_configurated(value: str):
    # it's configurated
    if value.startswith('99'):
        return True
    else:
        return False


def is_9H(value: str):
    if '9H' in value:
        return True
    else:
        return False


def is_csr(value):
    if value == "CSR":
        return True
    else:
        return False


def main_cohv_logic_function(logic_parameters):
    """
    conditions:
        condition1: Everything that is "CSR" and has a stocklevel has to be skipped.
        condition2: If it´s configurated or there is no stock, then it has to be converted
        condition3: If in the text is the string "9H", And it´s configurated AND it´s more than 1 pcs, then don´t convert
        con
    :param logic_parameters:
    :return: False - skip, True - convert
    """
    factors = dict()

    if logic_parameters['FEVOR_is_csr'] and not logic_parameters['LABST_is_zero']:
        factors['condition1'] = False
    else:
        factors['condition1'] = True

    if logic_parameters['MATNR_is_configurated'] or logic_parameters['LABST_is_zero']:
        factors['condition2'] = True
    else:
        factors['condition2'] = False

    if logic_parameters['MATXT_is_9H'] and logic_parameters['MATNR_is_configurated'] and not logic_parameters['GAMNG_is_one']:
        factors['condition3'] = False
    else:
        factors['condition3'] = True

    result = all(factors.values())
    return result


def select_and_convert(q, s_num, transaction, variant_name):
    """
    :param variant_name: variant of SAP transaction
    :param q: processing.Queue() object
    :param s_num: num of window on which to operate
    :param transaction: transaction which is opened on that window
    :return:
    """
    session = get_client(s_num, transaction)
    simple_load_variant(session, variant_name, False)

    # Check if there is any data
    pop_up_id = "wnd[1]/tbar[0]/btn[0]"
    if sap_element_exists(session, pop_up_id):
        session.findById(pop_up_id).press()
        sap_result = (dict(), dict(), "There wasn't any data.")
        q.put((variant_name, sap_result))
        return

    cohv_logic_factors = {"LABST": is_zero, "GAMNG": is_one, "MATNR": is_configurated, "MATXT": is_9H,
                          "FEVOR": is_csr}

    cohv_table_id = "wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell"

    # Format of the result: {'selected_orders': dict, 'skipped_orders': dict, 'sap_message': str}
    # result = select_rows_in_table("COHV", s_num, cohv_table_id, cohv_logic_factors, main_cohv_logic_function, RESULT_COL_NAMES, session)
    result = select_rows_in_table("COHV", s_num, cohv_table_id, cohv_logic_factors, main_cohv_logic_function, RESULT_COL_NAMES)

    # TODO: do the conversion if any order was selected
    if len(result['selected_orders']) > 0:
        cohv_mass_processing(session, "210", False)

    # TODO: load transaction again
    open_one_transaction(session, transaction)
    time.sleep(1)

    sap_result = (result['selected_orders'], result['skipped_orders'], result['sap_message'])
    q.put((variant_name, sap_result))


def load_remaining_orders(session, variant_name, planned_orders):
    """
    It loads in remaining orders to COHV.
    :param planned_orders: list of planned orders to be loaded in
    :param session: SAP session
    :param variant_name: name of variant, only to get appropriate layout
    :return:
    """
    if len(planned_orders) < 1:
        return

    insert_table_id = "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE"
    planned_orders_multiple_selection_button_id = "wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/btn%_S_PLNUM_%_APP_%-VALU_PUSH"

    open_one_transaction(session, "COHV")
    simple_load_variant(session, variant_name, True)

    # Clean the MRP disponents, and dates
    session.findById("wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/btn%_S_DISPO_%_APP_%-VALU_PUSH").press()
    session.findById("wnd[1]/tbar[0]/btn[16]").press()
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/btn%_S_TERST_%_APP_%-VALU_PUSH").press()
    session.findById("wnd[1]/tbar[0]/btn[16]").press()
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/btn%_S_RTERST_%_APP_%-VALU_PUSH").press()
    session.findById("wnd[1]/tbar[0]/btn[16]").press()
    session.findById("wnd[1]/tbar[0]/btn[8]").press()

    # insert planned orders to variant
    insert_production_orders(planned_orders, session, planned_orders_multiple_selection_button_id, insert_table_id)

    # Load variant in
    session.findById("wnd[0]").sendVKey(8)


if __name__ == "__main__":
    username = os.getlogin()
    status_file = (f"C:/Users/{username}/OneDrive - Roto Frank DST/General/05_Automatyzacja_narzędzia/100_STATUS"
                   f"/02_AUTOMATION_TOOLS_STATUS_BMH.xlsx")

    today = datetime.today().strftime("%Y_%m_%d")
    start_time = datetime.now().strftime("%H:%M:%S")

    file_paths = {
        "converted_positions": f"historical_data/converted_positions_{today}.xlsx",
        "skipped_positions": f"historical_data/skipped_positions_{today}.xlsx",
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

    # queue = multiprocessing.Queue()  # Create a shared queue
    manager = multiprocessing.Manager()
    queue = manager.Queue()
    processes = []

    try:
        sess1, tr1, nu1 = get_last_session(max_num_of_sessions=4)
        sess2, tr2, nu2 = get_last_session(max_num_of_sessions=5)
        sess3, tr3, nu3 = get_last_session(max_num_of_sessions=6)
        sessions = [sess1, sess2, sess3]
        sess_transactions = [tr1, tr2, tr3]
        sess_nums = [nu1, nu2, nu3]

        for sess in sessions:
            open_one_transaction(sess, "COHV")

        # run cohv conversion concurrently
        for variant, sess_num, tr in zip(VARIANT_NAMES, cycle(sess_nums), cycle(sess_transactions)):
            process = multiprocessing.Process(
                target=select_and_convert, args=(queue, sess_num, "COHV", variant)
            )
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        # simple_load_variant(sess1, VARIANT_NAME, False)
        # result = select_rows_in_table("COHV", nu1, COHV_TABLE_ID, COHV_STOCK_COL_NAME, RESULT_COL_NAMES, sess1)

        # do the conversion
        # cohv_mass_processing(sess1, "210", False)

        result_converted_positions = {key: [] for key in RESULT_COL_NAMES}
        result_skipped_positions = {key: [] for key in RESULT_COL_NAMES}
        result_sap_messages = dict()

        # Collect operation statuses into dictionaries
        while not queue.empty():
            variant, sap_data = queue.get()
            for key in result_converted_positions:
                result_converted_positions[key].extend(sap_data[0].get(key, []))
            for key in result_skipped_positions:
                result_skipped_positions[key].extend(sap_data[1].get(key, []))
            result_sap_messages[variant] = sap_data[2]

        #  save results to file
        df_convrted = pd.DataFrame(result_converted_positions)
        df_convrted.to_excel(paths['converted_positions'])
        df_skipped = pd.DataFrame(result_skipped_positions)
        df_skipped.to_excel(paths['skipped_positions'])

        load_remaining_orders(session=sess3, variant_name=VARIANT_NAMES[0], planned_orders=df_skipped['AUFNR'].to_list())

        # Handle the information for status file
        total_gamng = int(pd.to_numeric(df_convrted['GAMNG'], errors='coerce').sum())
        program_status['COHV_CONVERSION_SUMMARY'] = (f"In total {df_convrted.shape[0]} rows converted. Total sum of "
                                                     f"converted items: {total_gamng}.")
        program_status['COHV_CONVERTED_LINK'] = f"Details of converted items: {paths['converted_positions']}"
        program_status['COHV_SKIPPED_LINK'] = f"Details of skipped items: {paths['skipped_positions']}"
        program_status['COHV_CONVERSION_SYSTEM_MESSAGE'] = result_sap_messages

    except Exception as e:
        print(e)
        logging.error("Error occurred", exc_info=True)

    finally:
        # Fill status file
        end_time = datetime.now().strftime("%H:%M:%S")
        program_status['start_time'] = start_time
        program_status['end_time'] = end_time
        append_status_to_excel(status_file, program_status, ERROR_LOG_PATH, sheet_name="COHV_CONVERSION")

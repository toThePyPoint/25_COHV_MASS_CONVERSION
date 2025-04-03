import multiprocessing
import sys
import time
import psutil

from openpyxl import load_workbook

from sap_connection import get_client
from other_functions import close_excel_file


def load_variant(variant_name, session_idx, name_of_transaction, open_only, close_sap=False, current_transaction="SESSION_MANAGER"):
    """
    :param variant_name: Name of SAP variant :param session_idx: Which session should be selected :param
    name_of_transaction: Name of SAP transaction :param open_only: (True or False) if True transaction will be
    selected but not loaded in :param close_sap: Used to close SAP if True program will close SAP by entering "/nEX"
    to first session (provided that name_of_transaction matches name of transaction in first session) :return:
    """
    if close_sap:
        obj_sess = get_client(0, transaction=name_of_transaction)
        name_of_transaction = "EX"
    else:
        obj_sess = get_client(session_idx, transaction=current_transaction)
    # print(f"Loading variant {variant_name}")
    # obj_sess.findById("wnd[0]").maximize()
    obj_sess.findById("wnd[0]/tbar[0]/okcd").text = "/n" + name_of_transaction
    obj_sess.findById("wnd[0]").sendVKey(0)

    if not variant_name:
        return

    obj_sess.findById("wnd[0]").sendVKey(17)
    # obj_sess.StartTransaction("COHV")
    obj_sess.findById("wnd[1]/usr/txtV-LOW").text = variant_name
    obj_sess.findById("wnd[1]/usr/txtENAME-LOW").text = ""
    obj_sess.findById("wnd[1]/usr/txtV-LOW").caretPosition = 9
    obj_sess.findById("wnd[1]/tbar[0]/btn[8]").press()

    if open_only:
        return

    obj_sess.findById("wnd[0]").sendVKey(8)


def simple_load_variant(obj_sess, variant_name, open_only=False):
    obj_sess.findById("wnd[0]").sendVKey(17)
    # obj_sess.StartTransaction("COHV")
    obj_sess.findById("wnd[1]/usr/txtV-LOW").text = variant_name
    obj_sess.findById("wnd[1]/usr/txtENAME-LOW").text = ""
    obj_sess.findById("wnd[1]/usr/txtV-LOW").caretPosition = 9
    obj_sess.findById("wnd[1]/tbar[0]/btn[8]").press()

    if open_only:
        return

    obj_sess.findById("wnd[0]").sendVKey(8)


def create_new_sessions(variants_list, max_run_time):
    # If there is only one variant we hold on the program until everything is loaded in
    if len(variants_list) < 2:
        obj_sess = None
        start_time = time.time()
        while not obj_sess:
            obj_sess = get_client()
            elapsed_time = time.time() - start_time

            if elapsed_time > max_run_time:
                print("Program has been running for too long. Exiting.")
                sys.exit()

    for variant in variants_list[:-1]:
        obj_sess = None
        start_time = time.time()
        while not obj_sess:
            obj_sess = get_client()
            elapsed_time = time.time() - start_time

            if elapsed_time > max_run_time:
                print("Program has been running for too long. Exiting.")
                sys.exit()
        obj_sess.createSession()


def open_transactions(variants, transactions, open_only_modes):
    """
    :param variants: List of SAP variants
    :param transactions: List of SAP transactions
    :param open_only_modes: List of only_mode parameters (boolean).
            True means that transaction will be selected but not loaded in
    :return:
    """
    max_run_time = 60

    processes = []

    time.sleep(1)
    create_new_sessions(variants, max_run_time)

    for session_idx, parameters in enumerate(zip(variants, transactions, open_only_modes)):
        variant = parameters[0]
        transaction = parameters[1]
        open_only_mode = parameters[2]
        process = multiprocessing.Process(target=load_variant, args=(variant, session_idx, transaction, open_only_mode))
        processes.append(process)
        process.start()
        time.sleep(0.5)

    for process in processes:
        process.join()


def get_values_from_table(transaction, num_of_window, table_id, column_names, session=None):
    if not session:
        obj_sess = get_client(num_of_window, transaction)
    else:
        obj_sess = session

    table = obj_sess.findById(table_id)
    row_count = table.RowCount
    visible_rows = table.VisibleRowCount

    retrieved_values = dict()
    # idx = 0

    current_row = 0
    while current_row < row_count:
        # Set the first visible row to the current row index
        table.firstVisibleRow = current_row

        # Read rows currently visible
        for i in range(visible_rows):
            if current_row + i == row_count:
                break
            for column_name in column_names:
                table_value = table.GetCellValue(current_row + i, column_name)
                retrieved_values.setdefault(column_name, []).append(table_value)

            # # idx += 1
            # # print(f"Order num: {idx} | order value: {order}")
            # if order:
            #     orders.append(order)

    #     Scroll down
        current_row += visible_rows

    return retrieved_values


def insert_production_orders(production_orders, session, prod_ord_multiple_selection_btn_id, table_id):
    session.findById(prod_ord_multiple_selection_btn_id).press()
    visible_rows = session.findById(table_id).VisibleRowCount
    # print("Visible rows count:", visible_rows)

    length_of_input_list = len(production_orders)

    current_row = 0
    while current_row < length_of_input_list:

        for idx, order in enumerate(production_orders[current_row:current_row + (visible_rows - 1)], start=1):
            session.findById(f"{table_id}/ctxtRSCSEL_255-SLOW_I[1,{idx}]").text = str(order)

        current_row += (visible_rows - 1)
        if current_row < length_of_input_list:
            session.findById(table_id).verticalScrollbar.position = current_row

    session.findById("wnd[1]/tbar[0]/btn[8]").press()


def export_data_to_file(transaction, num_of_window, file_path, file_name):
    obj_sess = get_client(num_of_window, transaction)

    obj_sess.findById("wnd[0]/tbar[1]/btn[43]").press()
    obj_sess.findById("wnd[1]/usr/cmbG_LISTBOX").setFocus()
    obj_sess.findById("wnd[1]/usr/cmbG_LISTBOX").key = "31"
    obj_sess.findById("wnd[1]/tbar[0]/btn[0]").press()
    obj_sess.findById("wnd[1]/usr/ctxtDY_PATH").text = file_path
    obj_sess.findById("wnd[1]/usr/ctxtDY_FILENAME").text = file_name
    obj_sess.findById("wnd[1]/usr/ctxtDY_PATH").setFocus()
    obj_sess.findById("wnd[1]/usr/ctxtDY_PATH").caretPosition = 74
    obj_sess.findById("wnd[1]/tbar[0]/btn[11]").press()

    time.sleep(2)
    # Iterate over all running processes
    close_excel_file(file_name)


def open_one_transaction(session, transaction_name):
    session.findById("wnd[0]/tbar[0]/okcd").text = "/n" + transaction_name
    session.findById("wnd[0]").sendVKey(0)


def clear_sap_warnings(session):
    """
    Check for SAP warning messages and clear them if present.
    """
    try:
        message_bar = session.findById("wnd[0]/sbar")
        if message_bar.MessageType == "W":  # 'W' stands for Warning (Yellow message)
            session.findById("wnd[0]").sendVKey(0)  # Press Enter to acknowledge the warning
            time.sleep(0.2)  # Give SAP some time to process
            # print("SAP warning cleared.")
    except Exception as e:
        print(f"Error handling SAP message: {e}")


def get_sap_message(session):
    """
    Retrieve the text of the SAP message from the status bar.
    """
    try:
        message_bar = session.findById("wnd[0]/sbar")
        return message_bar.Text  # Return the message text
    except Exception as e:
        print(f"Error retrieving SAP message: {e}")
        return None  # Return None if there's an error

import os
import sys
import time
import multiprocessing

import win32com.client
import subprocess


def get_client(num_of_window=0, transaction="SESSION_MANAGER"):
    """
    :param transaction: name of transaction for which the session will be returned, 'SESSION_MANAGER' for empty window
    :param num_of_window: if there are more than one SESSION_MANAGERS, method will return then num_of_window(th) in
    sequence.
    :return:
    """
    sap_gui_auto = win32com.client.GetObject("SAPGUI")
    if not type(sap_gui_auto) == win32com.client.CDispatch:
        return

    application = sap_gui_auto.GetScriptingEngine
    if not type(application) == win32com.client.CDispatch:
        sap_gui_auto = None
        return

    for conn in range(application.Children.Count):
        # Loop through the application and get the connection
        connection = application.Children(conn)

    for idx, sess in enumerate(range(connection.Children.Count)):
        # print(idx)
        # Loop through each connection and return sessions that are on the main screen 'SESSION_MANAGER'
        session = connection.Children(sess)

        if session.Info.Transaction == transaction and idx == num_of_window:
            return session
        else:
            # Return None and break
            pass
            # return


def open_sap():
    # Path to your SAP GUI executable (e.g., sapgui.exe)
    sap_gui_path = r"C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe"
    sap_gui_path_alternative = r"C:\Appls\SAP\FrontEnd\SAPgui\saplogon.exe"

    if not os.path.exists(sap_gui_path):
        sap_gui_path = sap_gui_path_alternative

    # Check if the file exists
    if os.path.exists(sap_gui_path):
        # Launch SAP GUI
        subprocess.Popen(sap_gui_path)
    else:
        print(f"Error: SAP GUI not found at {sap_gui_path}")

    time.sleep(2)


def sap_log_in(sap_system):
    # Initialize SAP GUI scripting engine
    SapGuiAuto = win32com.client.GetObject("SAPGUI")
    application = SapGuiAuto.GetScriptingEngine

    # Open connection to SAP using a system identifier (no need for user credentials if SSO is enabled)
    connection = application.OpenConnection(sap_system, True)

    # Access the first session after connecting
    session = connection.Children(0)

    # Perform further actions (e.g., navigating in SAP) – no need to manually log in if SSO is set up
    # session.findById("wnd[0]").maximize()  # Maximize the SAP window


def get_last_sap_window(max_num_of_sessions=6):
    """
    :return: last_num_of_window: how many SAP windows are opened,
    last_session: session opened in last window
    last_transaction: transaction opened in last window
    """
    last_num_of_window = None
    last_session = None
    last_transaction = None

    sap_gui_auto = win32com.client.GetObject("SAPGUI")
    if not type(sap_gui_auto) == win32com.client.CDispatch:
        return

    application = sap_gui_auto.GetScriptingEngine
    if not type(application) == win32com.client.CDispatch:
        sap_gui_auto = None
        return

    for conn in range(application.Children.Count):
        # Loop through the application and get the connection
        connection = application.Children(conn)

    for idx, sess in enumerate(range(connection.Children.Count)):
        # print(idx)
        # Loop through each connection and return sessions that are on the main screen 'SESSION_MANAGER'
        session = connection.Children(sess)
        last_session = session
        last_num_of_window = idx
        last_transaction = session.Info.Transaction
        if idx == max_num_of_sessions - 1:
            # it enables us to be more flexible and select session which is not last session
            # by adjusting max_num_of_session parameter in get_last_session method
            break

    return last_num_of_window, last_session, last_transaction


def get_last_session(max_num_of_sessions=6):
    """
    Function returns last session (if there are 6 SAP windows opened, otherwise it creates new SESSION_MANAGER and returns it
    :param max_num_of_sessions: how many sessions can be opened in SAP simultaneously
    :return: SAP session, transaction and num of window of session
    """
    # We assume the SAP is already opened
    last_num_of_window, last_session, last_transaction = get_last_sap_window(max_num_of_sessions)
    if last_num_of_window >= max_num_of_sessions - 1:
        # operate on last session
        sess = last_session
        transaction = last_transaction
        num_of_window = last_num_of_window
    else:
        # create new session
        last_session.createSession()
        time.sleep(2)
        last_num_of_window += 1
        sess = get_client(last_num_of_window)
        transaction = "SESSION_MANAGER"
        num_of_window = last_num_of_window

    return sess, transaction, num_of_window

import re
import time

import pyperclip
import pywintypes
from sap_functions import clear_sap_warnings, get_sap_message


def pk03_get_container_data(mat_nr, plant, prod_supply_area, session):
    session.findById("wnd[0]/usr/ctxtRMPKR-MATNR").text = mat_nr
    session.findById("wnd[0]/usr/ctxtRMPKR-WERKS").text = plant
    session.findById("wnd[0]/usr/ctxtRMPKR-PRVBE").text = prod_supply_area

    session.findById("wnd[0]").sendVKey(0)

    try:
        size_of_container = session.findById("wnd[0]/usr/txtPKHD-BEHMG").text
        number_of_containers = session.findById("wnd[0]/usr/txtPKHD-BEHAZ").text
    except Exception as e:
        print(e)
        size_of_container = None
        number_of_containers = None
        return size_of_container, number_of_containers

    # Leave transaction (go back - green arrow (F3- button shortcut))
    session.findById("wnd[0]/tbar[0]/btn[3]").press()

    return size_of_container, number_of_containers


def pk02_set_container_data(mat_nr, plant, prod_supply_area, session, size_of_container, number_of_containers,
                            previous_num_of_containers):
    """
    :param mat_nr:
    :param plant:
    :param prod_supply_area:
    :param session:
    :param size_of_container: new container size
    :param number_of_containers: new number of containers
    :param previous_num_of_containers: current (previous) number of containers - number of containers before the change
    :return:
    """
    session.findById("wnd[0]/usr/ctxtRMPKR-MATNR").text = mat_nr
    session.findById("wnd[0]/usr/ctxtRMPKR-WERKS").text = plant
    session.findById("wnd[0]/usr/ctxtRMPKR-PRVBE").text = prod_supply_area

    session.findById("wnd[0]").sendVKey(0)

    session.findById("wnd[0]/usr/txtPKHD-BEHAZ").text = str(number_of_containers)
    session.findById("wnd[0]/usr/txtPKHD-BEHMG").text = str(size_of_container)

    if int(number_of_containers) == 1:
        session.findById("wnd[0]/usr/txtPKHD-BEHAZ").text = "2"

    # delete needless container/s
    session.findById("wnd[0]/tbar[1]/btn[6]").press()

    num_of_containers_diff = previous_num_of_containers - number_of_containers
    if num_of_containers_diff > 0:
        for container_idx_offset in range(0, num_of_containers_diff):
            container_idx = previous_num_of_containers - 1 - container_idx_offset
            session.findById(f"wnd[1]/usr/tblSAPMMPKRKANBANLIST/chkPKPS-SPKKZ[0,{container_idx}]").selected = True
            session.findById(f"wnd[1]/usr/tblSAPMMPKRKANBANLIST/chkRMPKR-LOEKZ[1,{container_idx}]").selected = True
            session.findById(f"wnd[1]/usr/tblSAPMMPKRKANBANLIST/chkRMPKR-LOEKZ[1,{container_idx}]").setFocus()

    # 2 to 1 || 2 to 2 containers implementation
    if int(number_of_containers) == 2:
        # new_num_of_containers = 2
        # --> unlock second container
        session.findById("wnd[1]/usr/tblSAPMMPKRKANBANLIST/chkPKPS-SPKKZ[0,1]").selected = False

    if int(number_of_containers) == 1:
        # new_num_of_containers = 1
        # --> lock second container
        # --> uncheck deletion mark
        session.findById("wnd[1]/usr/tblSAPMMPKRKANBANLIST/chkPKPS-SPKKZ[0,1]").selected = True
        session.findById("wnd[1]/usr/tblSAPMMPKRKANBANLIST/chkRMPKR-LOEKZ[1,1]").selected = False

    session.findById("wnd[1]/tbar[0]/btn[7]").press()

    # save changes
    session.findById("wnd[0]/tbar[0]/btn[11]").press()


def pk31_change_container_status(mat_nr, plant, prod_supply_area, session, container_idx, new_status):
    """
    :param mat_nr:
    :param plant:
    :param prod_supply_area:
    :param session:
    :param container_idx: 0 for first, so if num of containers is about to be decreased from 5 to 4, idx = 4
    :param new_status: 1 for 'waiting', 2 for 'empty'
    :return:
    """

    session.findById("wnd[0]/usr/ctxtRMPKB-MATNR").text = mat_nr
    session.findById("wnd[0]/usr/ctxtRMPKB-WERKS").text = plant
    session.findById("wnd[0]/usr/ctxtRMPKB-PRVBE").text = prod_supply_area

    session.findById("wnd[0]").sendVKey(0)

    session.findById(f"wnd[0]/usr/tblSAPLMPKPKANBANLIST3/txtPKPS-PKPOS[1,{container_idx}]").setFocus()
    session.findById(f"wnd[0]/usr/tblSAPLMPKPKANBANLIST3/txtPKPS-PKPOS[1,{container_idx}]").caretPosition = 1
    session.findById("wnd[0]").sendVKey(2)

    session.findById("wnd[0]/usr/subINCLUDE440:SAPLMPKP:0440/ctxtRMPKB-PKBST").text = new_status
    session.findById("wnd[0]/usr/subINCLUDE440:SAPLMPKP:0440/ctxtRMPKB-PKBST").setFocus()
    session.findById("wnd[0]/usr/subINCLUDE440:SAPLMPKP:0440/ctxtRMPKB-PKBST").caretPosition = 1

    session.findById("wnd[0]").sendVKey(0)
    session.findById("wnd[0]/usr/subINCLUDE440:SAPLMPKP:0440/btnPUSH_ST").press()
    session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    # save changes
    session.findById("wnd[0]/tbar[0]/btn[3]").press()


def zfauf_create_production_orders(session, file_path):
    """
    :param session: SAP session
    :param file_path: txt_file_path with data to ZFAUF transaction
    :return: status "OK" or error which occured
    """
    try:
        session.findById("wnd[0]/usr/ctxtFILENAME").text = file_path
        session.findById("wnd[0]/tbar[1]/btn[8]").press()
        return "OK"
    except Exception as e:
        return f"Exception: {str(e)}"


def zpp_cserie_insert_data_to_table(session, new_values: dict, table_id, load_variant=False, save_orders=False):
    """
    :param session: SAP session to work with
    :param new_values: :dict: of new values to be inserted, keys must correspond to names of columns
    :param table_id:
    :param load_variant: boolean , determines if variant should be loaded first
    :param save_orders: boolean , determines if production orders should be saved at the end(by clicking 'Sichern Fauf')
    :return: "OK" if successful, or an error message if an error occurs.
    """
    try:
        if load_variant:
            session.findById("wnd[0]/tbar[1]/btn[8]").press()

        column_names = list(new_values.keys())

        table = session.findById(table_id)
        row_count = table.RowCount
        visible_rows = table.VisibleRowCount

        current_row = 0
        counter = 0
        while current_row < row_count:
            # Set the first visible row to the current row index
            table.firstVisibleRow = current_row

            # Insert to rows currently visible
            for i in range(visible_rows):
                if current_row + i == row_count:
                    break
                for column_name in column_names:
                    table.modifyCell(current_row + i, str(column_name), str(new_values[column_name][counter]))
                    counter += 1

            #     Scroll down
            current_row += visible_rows

        if save_orders:
            session.findById("wnd[0]/tbar[1]/btn[32]").press()

    except Exception as e:
        return f"Exception: {str(e)}"

    return "OK"


def cohv_select_system_status(session, sys_status=1, selection_exclude=False, load_transaction=False):
    """
    :param session: SAP session
    :param sys_status: 14 - DSTR, 11 - POTW etc.
    :param selection_exclude: :boolean: True means that selected status will be excluded
    :param load_transaction:
    :return:
    """
    session.findById(
        "wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/ctxtP_SYST1").setFocus()
    session.findById(
        "wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/ctxtP_SYST1").caretPosition = 3
    session.findById("wnd[0]").sendVKey(4)
    session.findById(f"wnd[1]/usr/lbl[1,{sys_status}]").setFocus()
    session.findById(f"wnd[1]/usr/lbl[1,{sys_status}]").caretPosition = 1
    session.findById("wnd[1]").sendVKey(2)

    session.findById(
        "wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/chkP_KZ_E1").selected = selection_exclude

    if load_transaction:
        session.findById("wnd[0]").sendVKey(8)


def cohv_mass_processing(session, type_of_operation, select_all=True):
    """
    :param select_all: determines if all rows should be selected
    :param session:
    :param type_of_operation: :str: "200" - orders confirmation, "130" - release orders, "210" - convert PlOrd To PrdOrd
    :return:
    """
    if select_all:
        session.findById("wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell").setCurrentCell(-1, "")
        session.findById("wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell").selectAll()
    session.findById("wnd[0]/mbar/menu[4]/menu[1]").select()

    session.findById("wnd[1]/usr/subFUNCTION_SETUP:SAPLCOWORK:0200/cmbCOWORK_FCT_SETUP-FUNCT").key = type_of_operation
    session.findById("wnd[1]/tbar[0]/btn[8]").press()


def partial_matching(sap_session, id_element_tag, id_root_pattern=None, id_root="wnd[0]/usr"):
    """
    Recursively searches for an SAP GUI element within a container using a flexible root ID pattern.

    :param id_root:
    :param sap_session: Active SAP GUI session object.
    :param id_root_pattern: Regex pattern to match the root ID (e.g., "wnd[0]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\\d+").
    :param id_element_tag: The unique part of the element ID to search for (e.g., "txtGOITEM-ERFMG").
    :return: The full ID of the matched element if found, otherwise None.
    """

    try:
        # root_container = sap_session.findById(id_root)

        # Search for a matching root ID dynamically
        if id_root_pattern:
            # for element in root_container.Children:
            #     print(f"Checking element: {element.Id}")  # Debugging output
            #     if re.search(id_root_pattern, element.Id):
            #         matched_root_id = element.Id
            #         return recursive_search(sap_session, matched_root_id, id_element_tag)
            matched_root_id = recursive_search(sap_session, id_root, id_root_pattern)
            if matched_root_id:
                return recursive_search(sap_session, matched_root_id, id_element_tag)

            print("Matching root ID not found!")
            return None

        else:
            # container = sap_session.findById(id_root)
            return recursive_search(sap_session, id_root, id_element_tag)

    except Exception as e:
        print(f"Error finding element: {e}")
        return None


def recursive_search(sap_session, container_id, id_element_tag):
    """
    Helper function to recursively search within SAP GUI container elements.

    :param sap_session: Active SAP GUI session object.
    :param container_id: ID of the container to search within.
    :param id_element_tag: The unique part of the element ID to match.
    :return: The matched element ID if found, otherwise None.
    """
    try:
        container = sap_session.findById(container_id)
        for element in container.Children:
            # print(f"Checking element: {element.Id}")  # Debugging output

            # Check if the current element ID contains the desired tag
            if re.search(rf"{id_element_tag}", element.Id):
                # print(f"Found match: {element.Id}")
                return element.Id

            # Recursively check child elements
            if hasattr(element, 'Children') and len(element.Children) > 0:
                found_id = recursive_search(sap_session, element.Id, id_element_tag)
                if found_id:
                    return found_id

        return None  # No match found in this branch
    except Exception as e:
        print(f"Error searching in container {container_id}: {e}")
        return None


def migo_instantiate_booking(session, mat_nr, document_header, quantity, plant, storage_loc, cost_center):
    field_id = partial_matching(session, r"cmbGODYNPRO")
    if field_id:
        session.findById(field_id).key = "A07"
    else:
        print('Booking Type field missing!')
        return

    # Click the "Item Details" button
    item_detail_btn_id = None
    try:
        item_detail_btn_id = partial_matching(session, r"btnBUTTON_DETAIL")
    except Exception as e:
        print(e)
    if item_detail_btn_id:
        session.findById(item_detail_btn_id).press()
    else:
        print("Item details button not found!")

    # Select the "Material" tab
    material_tab_id = partial_matching(session, r"tabpOK_GOITEM_MATERIAL")
    if material_tab_id:
        session.findById(material_tab_id).select()
    else:
        print("Material tab not found!")
        return

    # Set the Document Header text field
    document_header_id = partial_matching(session, r"txtGOHEAD-BKTXT")
    if document_header_id:
        session.findById(document_header_id).text = document_header
    else:
        print("Document header text field not found!")
        return

    # Set the Material Number field
    material_number_id = partial_matching(session, r"ctxtGOITEM-MAKTX")
    if material_number_id:
        session.findById(material_number_id).text = str(mat_nr)
    else:
        print("Material number field not found!")
        return

    # Press the Enter key
    session.findById("wnd[0]").sendVKey(0)

    # Select the "Quantities" tab
    quantities_tab_id = partial_matching(session, r"tabpOK_GOITEM_QUANTITIES")
    if quantities_tab_id:
        session.findById(quantities_tab_id).select()
    else:
        print("Quantities tab not found!")
        return

    quantity_field_id = partial_matching(
        session,
        r"txtGOITEM-ERFMG",
        r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMDETAIL:SAPLMIGO:\d+/subSUB_DETAIL:SAPLMIGO:\d+"
    )
    if quantity_field_id:
        print(f"Found Quantity Field ID: {quantity_field_id}")
        session.findById(quantity_field_id).text = str(quantity)
    else:
        print("Quantity field not found!")
        return

    # Select the "Destination" tab
    dest_tab_id = partial_matching(session, r"tabpOK_GOITEM_DESTINAT.")
    if dest_tab_id:
        session.findById(dest_tab_id).select()
    else:
        print("Destination tab not found!")
        return

    # Set the Plant field
    plant_field_id = partial_matching(
        session,
        r"ctxtGOITEM-NAME1",
        r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMDETAIL:SAPLMIGO:\d+/subSUB_DETAIL:SAPLMIGO:\d+"
    )
    if plant_field_id:
        session.findById(plant_field_id).text = str(plant)
    else:
        print("Plant field not found!")
        return

    # Set the Storage Location field
    storage_loc_id = partial_matching(
        session,
        r"ctxtGOITEM-LGOBE",
        r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMDETAIL:SAPLMIGO:\d+/subSUB_DETAIL:SAPLMIGO:\d+"
    )
    if storage_loc_id:
        session.findById(storage_loc_id).text = storage_loc
    else:
        print("Storage location field not found!")
        return

    # Set the Document Header field
    document_header_id = partial_matching(
        session,
        r"txtGOITEM-SGTXT",
        r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMDETAIL:SAPLMIGO:\d+/subSUB_DETAIL:SAPLMIGO:\d+"
    )
    if document_header_id:
        session.findById(document_header_id).text = document_header
    else:
        print("Document header field not found!")
        return

    # Press the Enter key
    session.findById("wnd[0]").sendVKey(0)

    # Select the "Account" tab
    account_tab_id = partial_matching(session, r"tabpOK_GOITEM_ACCOUNT")
    if account_tab_id:
        session.findById(account_tab_id).select()
    else:
        print("Account tab not found!")
        return

    # Set the Cost Center field
    cost_center_id = partial_matching(
        session,
        r"ctxtCOBL-KOSTL",
        r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMDETAIL:SAPLMIGO:\d+/subSUB_DETAIL:SAPLMIGO:\d+"
    )
    if cost_center_id:
        session.findById(cost_center_id).text = str(cost_center)
    else:
        print("Cost center field not found!")
        return

    # Press the Enter key
    session.findById("wnd[0]").sendVKey(0)

    # Click the "Next Item" button
    next_item_btn_id = partial_matching(session, r"btnOK_NEXT_ITEM")
    if next_item_btn_id:
        session.findById(next_item_btn_id).press()
    else:
        print("Next item button not found!")
        return

    # Click the "Item Details" button
    item_detail_btn_id = partial_matching(session, r"btnBUTTON_ITEMDETAIL")
    if item_detail_btn_id:
        session.findById(item_detail_btn_id).press()
    else:
        print("Item details button not found!")
        return


def migo_fill_table_matnr_quantity(session, df):
    table_id = partial_matching(session, "tblSAPLMIGOTV_GOITEM",
                                r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMLIST:SAPLMIGO:\d+")

    # Dynamic SAP GUI element IDs
    index = 0
    matnr_id = partial_matching(session, rf"ctxtGOITEM-MAKTX\[4,{index}\]",
                                r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMLIST:SAPLMIGO:\d+/tblSAPLMIGOTV_GOITEM")
    menge_id = partial_matching(session, rf"txtGOITEM-ERFMG\[5,{index}\]",
                                r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMLIST:SAPLMIGO:\d+/tblSAPLMIGOTV_GOITEM")

    table = session.FindById(table_id)
    # Retrieve visible row count
    visible_rows = table.visibleRowCount

    index_offset = 0

    for index, row in df.iterrows():
        matnr_id = str.replace(matnr_id, f"[4,{index - 1 - index_offset}]", f"[4,{index - index_offset}]")
        menge_id = str.replace(menge_id, f"[5,{index - 1 - index_offset}]", f"[5,{index - index_offset}]")

        session.findById(matnr_id).text = str(row['MatNR'])
        session.findById(menge_id).text = str(row['Menge'])

        if index == visible_rows - 1 + index_offset:
            session.findById(table_id).verticalScrollbar.position += 1
            session.findById(table_id).verticalScrollbar.position = visible_rows + index_offset - 1
            index_offset += (visible_rows - 1)
            matnr_id = str.replace(matnr_id, f"[4,{visible_rows - 1}]", f"[4,{index - index_offset}]")
            menge_id = str.replace(menge_id, f"[5,{visible_rows - 1}]", f"[5,{index - index_offset}]")
            time.sleep(0.2)

    pos = session.findById(table_id).verticalScrollbar.position
    while pos > 0:
        session.findById("wnd[0]").sendVKey(81)  # Page Up key
        time.sleep(0.2)
        pos = session.findById(table_id).verticalScrollbar.position


def migo_fill_columns_down(session, cols_to_be_filled_down):
    # Fill the data down the specified columns
    take_value_btn_id = partial_matching(session, r"btnOK_TAKE_VALUE")
    table_id = partial_matching(session, "tblSAPLMIGOTV_GOITEM",
                                r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMLIST:SAPLMIGO:\d+")

    for col in cols_to_be_filled_down:
        # col_id = partial_matching(sap_session=session,
        #                           id_element_tag=col,
        #                           id_root_pattern=r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMLIST:SAPLMIGO:\d+/tblSAPLMIGOTV_GOITEM")
        col_id = f"{table_id}/{col}"
        session.findById(col_id).setFocus()

        # Click the "Take Value" button
        if take_value_btn_id:
            session.findById(take_value_btn_id).press()
            time.sleep(0.2)
        else:
            print("Take value button not found!")
            return

    # Scroll table horizontally to the left
    col_id = f"{table_id}/{cols_to_be_filled_down[0]}"
    session.findById(col_id).setFocus()


def migo_update_storage_locations(session, df):
    table_id = partial_matching(session, "tblSAPLMIGOTV_GOITEM",
                                r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMLIST:SAPLMIGO:\d+")

    # Dynamic SAP GUI element IDs
    index = 0
    storage_loc_id = partial_matching(session, rf"ctxtGOITEM-LGOBE\[9,{index}\]",
                                      r"wnd\[0\]/usr/ssubSUB_MAIN_CARRIER:SAPLMIGO:\d+/subSUB_ITEMLIST:SAPLMIGO:\d+/tblSAPLMIGOTV_GOITEM")

    table = session.FindById(table_id)
    # Retrieve visible row count
    visible_rows = table.visibleRowCount

    index_offset = 0

    for index, row in df.iterrows():
        storage_loc_id = str.replace(storage_loc_id, f"[9,{index - 1 - index_offset}]", f"[9,{index - index_offset}]")

        session.findById(storage_loc_id).text = str(row['storage_loc'])

        if index == visible_rows - 1 + index_offset:
            session.findById(table_id).verticalScrollbar.position += 1
            session.findById(table_id).verticalScrollbar.position = visible_rows + index_offset - 1
            index_offset += (visible_rows - 1)
            storage_loc_id = str.replace(storage_loc_id, f"[9,{visible_rows - 1}]", f"[9,{index - index_offset}]")
            time.sleep(0.2)

    pos = session.findById(table_id).verticalScrollbar.position
    while pos > 0:
        session.findById("wnd[0]").sendVKey(81)  # Page Up key
        time.sleep(0.2)
        pos = session.findById(table_id).verticalScrollbar.position


def mb51_export_data_to_excel(session):
    session.findById("wnd[0]").maximize()
    session.findById("wnd[0]/mbar/menu[0]/menu[1]/menu[1]").select()
    session.findById("wnd[1]/usr/cmbG_LISTBOX").setFocus()
    session.findById("wnd[1]/usr/cmbG_LISTBOX").key = "33"
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").select()
    session.findById(
        "wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").setFocus()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()


def coois_export_data_to_excel(session):
    session.findById("wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell").pressToolbarButton(
        "&NAVIGATION_PROFILE_TOOLBAR_EXPAND")
    session.findById("wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
    session.findById("wnd[0]/usr/cntlCUSTOM/shellcont/shell/shellcont/shell").selectContextMenuItem("&XXL")
    session.findById("wnd[1]/usr/cmbG_LISTBOX").setFocus()
    session.findById("wnd[1]/usr/cmbG_LISTBOX").key = "33"
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()


def coois_load_orders_from_clipboard(session):
    session.findById(
        "wnd[0]/usr/tabsTABSTRIP_SELBLOCK/tabpSEL_00/ssub%_SUBSCREEN_SELBLOCK:PPIO_ENTRY:1200/btn%_S_AUFNR_%_APP_%-VALU_PUSH").press()
    session.findById("wnd[1]/tbar[0]/btn[24]").press()
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]/tbar[1]/btn[8]").press()


def mb51_load_matnrs_from_clipboard(session):
    session.findById("wnd[0]/usr/btn%_MATNR_%_APP_%-VALU_PUSH").press()
    session.findById("wnd[1]/tbar[0]/btn[24]").press()
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]").sendVKey(8)


def me21n_fill_table_with_delivery_orders_data(session, df, purchasing_dep, purchasing_group, business_unit,
                                               supplier='602100'):
    # Select standard order
    order_type_id = partial_matching(session, r"cmbMEPO_TOPLINE-BSART",
                                     r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB0:SAPLMEGUI:\d+/subSUB1:SAPLMEGUI:\d+")

    if order_type_id:
        session.findById(order_type_id).key = "NB"
    else:
        print("Delivery order type adjustment not needed")

    # Filling supplier code
    supplier_field_id = partial_matching(session, r"ctxtMEPO_TOPLINE-SUPERFIELD")
    if supplier_field_id:
        session.findById(supplier_field_id).text = supplier
    else:
        print('Supplier field missing!')
        return

    # Click the "Header" button
    header_btn_id = None
    try:
        header_btn_id = partial_matching(session, r"btnDYN_4000-BUTTON",
                                         r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB1:SAPLMEVIEWS:\d+/subSUB1:SAPLMEVIEWS:\d+")
    except Exception as e:
        print(e)
    if header_btn_id:
        session.findById(header_btn_id).press()
    else:
        print("Header button not found!")

    # Purchasing Department
    purchasing_dep_id = partial_matching(session, r"ctxtMEPO1222-EKORG")
    if purchasing_dep_id:
        session.findById(purchasing_dep_id).text = purchasing_dep
    else:
        print('Purchasing department field missing!')
        return

    # Purchasing Group
    purchasing_gr_id = partial_matching(session, r"ctxtMEPO1222-EKGRP")
    if purchasing_gr_id:
        session.findById(purchasing_gr_id).text = purchasing_group
    else:
        print('Purchasing group field missing!')
        return

    # Business Unit
    business_unit_id = partial_matching(session, r"ctxtMEPO1222-BUKRS")
    if business_unit_id:
        session.findById(business_unit_id).text = business_unit
    else:
        print('Business Unit field missing!')
        return

    # Dynamic SAP GUI element IDs
    index = 0
    matnr_id = partial_matching(session, rf"ctxtMEPO1211-EMATN\[4,{index}\]",
                                r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")

    quantity_id = partial_matching(session, rf"txtMEPO1211-MENGE\[6,{index}\]",
                                   r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")

    # name_id = partial_matching(session, rf"txtMEPO1211-TXZ01\[5,{index}\]",
    #                             r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")

    unit_id = partial_matching(session, rf"ctxtMEPO1211-MEINS\[7,{index}\]",
                               r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")

    type_id = partial_matching(session, rf"ctxtMEPO1211-ELPEI\[8,{index}\]",
                               r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")

    date_id = partial_matching(session, rf"ctxtMEPO1211-EEIND\[9,{index}\]",
                               r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")

    plant_id = partial_matching(session, rf"ctxtMEPO1211-NAME1\[15,{index}\]",
                                r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")

    table_id = partial_matching(session, "tblSAPLMEGUITC_1211",
                                r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+")

    table = session.FindById(table_id)
    # Retrieve visible row count
    visible_rows = table.visibleRowCount
    index_offset = 0

    # Click the "Position TAB" button
    position_tab_btn_id = partial_matching(session, r"btnDYN_4000-BUTTON",
                                           r"wnd\[0\]/usr/usr/subSUB0:SAPLMEGUI:0010/subSUB3:SAPLMEVIEWS:1100/subSUB1:SAPLMEVIEWS:4002")
    if position_tab_btn_id:
        session.findById(position_tab_btn_id).press()
    else:
        print("Position tab button not found!")

    for index, row in df.iterrows():
        matnr_id = str.replace(matnr_id, f"[4,{index - 1 - index_offset}]", f"[4,{index - index_offset}]")
        quantity_id = str.replace(quantity_id, f"[6,{index - 1 - index_offset}]", f"[6,{index - index_offset}]")
        # name_id = str.replace(name_id, f"[5,{index - 1 - index_offset}]", f"[5,{index - index_offset}]")
        unit_id = str.replace(unit_id, f"[7,{index - 1 - index_offset}]", f"[7,{index - index_offset}]")
        type_id = str.replace(type_id, f"[8,{index - 1 - index_offset}]", f"[8,{index - index_offset}]")
        date_id = str.replace(date_id, f"[9,{index - 1 - index_offset}]", f"[9,{index - index_offset}]")
        plant_id = str.replace(plant_id, f"[15,{index - 1 - index_offset}]", f"[15,{index - index_offset}]")

        session.findById(matnr_id).text = str(row['Material'])
        # session.findById(name_id).text = str(row['Description'])
        session.findById(quantity_id).text = str(row['Quantity'])
        session.findById(unit_id).text = str(row['Unit'])
        session.findById(type_id).text = str(row['Type'])
        session.findById(date_id).text = str(row['Date of delivery'])
        session.findById(plant_id).text = str(row['Plant'])

        if index == visible_rows - 1 + index_offset:
            # Get table ID once again as it's changed after scrolling (?)
            table_id = partial_matching(session, "tblSAPLMEGUITC_1211",
                                        r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+")
            session.findById(table_id).verticalScrollbar.position += 1

            for i in range(visible_rows):
                clear_sap_warnings(session)
            # Get table ID once again as it's changed after scrolling (?)
            table_id = partial_matching(session, "tblSAPLMEGUITC_1211",
                                        r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+")

            session.findById(table_id).verticalScrollbar.position = visible_rows + index_offset - 1
            index_offset += (visible_rows - 1)
            matnr_id = str.replace(matnr_id, f"[4,{visible_rows - 1}]", f"[4,{index - index_offset}]")
            quantity_id = str.replace(quantity_id, f"[6,{visible_rows - 1}]", f"[6,{index - index_offset}]")
            # name_id = str.replace(name_id, f"[5,{visible_rows - 1}]", f"[5,{index - index_offset}]")
            unit_id = str.replace(unit_id, f"[7,{visible_rows - 1}]", f"[7,{index - index_offset}]")
            type_id = str.replace(type_id, f"[8,{visible_rows - 1}]", f"[8,{index - index_offset}]")
            date_id = str.replace(date_id, f"[9,{visible_rows - 1}]", f"[9,{index - index_offset}]")
            plant_id = str.replace(plant_id, f"[15,{visible_rows - 1}]", f"[15,{index - index_offset}]")
            time.sleep(0.2)

            # Click the "Position TAB" button (because it can appear after scrolling)
            position_tab_btn_id = partial_matching(session, r"btnDYN_4000-BUTTON",
                                                   r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:0010/subSUB3:SAPLMEVIEWS:1100/subSUB1:SAPLMEVIEWS:4002")
            if position_tab_btn_id:
                session.findById(position_tab_btn_id).press()
            else:
                print("Position tab button not found!")

    session.findById("wnd[0]").sendVKey(0)  # Press Enter
    for i in range(visible_rows):
        clear_sap_warnings(session)


def co02_change_storage_location(session, new_storage_loc, auf_nr):
    """
    :param session: SAP Session
    :param new_storage_loc: str: eg. "0004"
    :param auf_nr: str: number of production order
    :return: "OK" if everything worked correctly or error if error emerged
    """
    try:
        session.findById("wnd[0]/usr/ctxtCAUFVD-AUFNR").text = str(auf_nr)
        session.findById("wnd[0]").sendVKey(0)
        session.findById("wnd[0]/usr/tabsTABSTRIP_0115/tabpKOWE").select()
        session.findById(
            "wnd[0]/usr/tabsTABSTRIP_0115/tabpKOWE/ssubSUBSCR_0115:SAPLCOKO1:0190/ctxtAFPOD-LGORT").text = new_storage_loc
        session.findById("wnd[0]/tbar[0]/btn[11]").press()
    except Exception as e:
        return f"Exception: {str(e)}"

    return "OK"


def me57_convert_purchase_requisitions(session, skip_stock_requisitions=True):
    """
    Method converts all existing purchase requisitions to purchase orders. It works for one plant (which should be
    preselected in variant). :param skip_stock_requisitions: boolean: If True It converts only pruchase requisitions
    for special customer requirements (with value "M") :param session: SAP session :return:
    """
    session.findById("wnd[0]/tbar[1]/btn[17]").press()
    session.findById("wnd[0]").sendVKey(2)
    session.findById("wnd[0]/usr/lbl[11,9]").setFocus()
    session.findById("wnd[0]/usr/lbl[11,9]").caretPosition = 2
    session.findById("wnd[0]").sendVKey(2)
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    # session.findById(
    #     "wnd[0]/usr/subSUB0:SAPLMEGUI:0015/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1211/tblSAPLMEGUITC_1211/btnMEPO1211-STATUSICON[0,11]").setFocus()
    # session.findById(
    #     "wnd[0]/usr/subSUB0:SAPLMEGUI:0015/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1211/tblSAPLMEGUITC_1211/btnMEPO1211-STATUSICON[0,11]").press()

    if skip_stock_requisitions:
        # Dynamic SAP GUI element IDs
        index = 0
        do_deletion = False

        # Typ Dekretacji w SAP - "M" oznacza zlec. kl.
        account_assignment_category_id = partial_matching(session, rf"ctxtMEPO1211-KNTTP\[2,{index}\]",
                                                          r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")
        matnr_id = partial_matching(session, rf"ctxtMEPO1211-EMATN\[4,{index}\]",
                                    r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+/tblSAPLMEGUITC_1211")
        table_id = partial_matching(session, "tblSAPLMEGUITC_1211",
                                    r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+")

        table = session.FindById(table_id)
        # Retrieve visible row count
        visible_rows = table.visibleRowCount
        index_offset = 0

        for index in range(visible_rows):
            account_assignment_category_id = str.replace(account_assignment_category_id, f"[2,{index - 1 - index_offset}]",
                                                         f"[2,{index - index_offset}]")
            matnr_id = str.replace(matnr_id, f"[4,{index - 1 - index_offset}]", f"[4,{index - index_offset}]")

            aac_value = session.findById(account_assignment_category_id).text
            mat_nr = session.findById(matnr_id).text
            if mat_nr == '':
                # Exit the loop if there is no data
                break
            if aac_value != "M":
                session.findById(table_id).getAbsoluteRow(index).selected = True
                do_deletion = True

            if index == visible_rows - 1 + index_offset:
                # Get table ID once again as it's changed after scrolling (?)
                table_id = partial_matching(session, "tblSAPLMEGUITC_1211",
                                            r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+")
                session.findById(table_id).verticalScrollbar.position += 1

                # for i in range(visible_rows):
                #     clear_sap_warnings(session)
                # Get table ID once again as it's changed after scrolling (?)
                table_id = partial_matching(session, "tblSAPLMEGUITC_1211",
                                            r"wnd\[0\]/usr/subSUB0:SAPLMEGUI:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB2:SAPLMEVIEWS:\d+/subSUB1:SAPLMEGUI:\d+")

                session.findById(table_id).verticalScrollbar.position = visible_rows + index_offset - 1
                index_offset += (visible_rows - 1)
                account_assignment_category_id = str.replace(account_assignment_category_id, f"[2,{visible_rows - 1}]",
                                                             f"[2,{index - index_offset}]")
                matnr_id = str.replace(matnr_id, f"[2,{visible_rows - 1}]", f"[2,{index - index_offset}]")
                time.sleep(0.2)

        if do_deletion:
            # Delete selected rows
            session.findById("wnd[0]/usr/subSUB0:SAPLMEGUI:0015/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:1211/btnDELETE").press()
            session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    # Save purchase order
    session.findById("wnd[0]").sendVKey(11)
    session.findById("wnd[1]/usr/btnSPOP-VAROPTION1").press()
    sap_message = get_sap_message(session)
    return sap_message


def md01_run_mrp(session):
    try:
        session.findById("wnd[0]/usr/chkRM61X-PARAL").selected = True
        session.findById("wnd[0]/usr/ctxtRM61X-PUWNR").text = "ZLUB"
        session.findById("wnd[0]/usr/ctxtRM61X-VERSL").text = "NETCH"
        session.findById("wnd[0]/usr/ctxtRM61X-BANER").text = "2"
        session.findById("wnd[0]/usr/ctxtRM61X-LIFKZ").text = "3"
        session.findById("wnd[0]/usr/ctxtRM61X-DISER").text = "1"
        session.findById("wnd[0]/usr/ctxtRM61X-PLMOD").text = "3"
        session.findById("wnd[0]/usr/ctxtRM61X-TRMPL").text = "2"
        session.findById("wnd[0]/usr/ctxtRM61X-UXKEY").text = "ZLU"

        # Send Enter key (0 = Enter)
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(0.2)
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(0.2)
        session.findById("wnd[0]").sendVKey(0)
        time.sleep(0.2)

        # Handle possible pop-up
        if session.Children.Count > 1:  # Check if a pop-up window appeared
            session.findById("wnd[1]").sendVKey(0)

    except Exception as e:
        return f"Exception: {str(e)}"

    return "MD01 MRP run launched successfully."


def zkbp1_copy_sap_grid_to_clipboard(session, columns):
    """
    Reads data from all rows in an SAP GUI grid, including those that require scrolling, and copies it to the clipboard.
    :param columns: columns to be copied from SAP table
    :param session: SAP session object
    """
    grid = session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell/shellcont[1]/shell")

    extracted_data = set()  # Use a set to avoid duplicate rows due to scrolling
    total_rows = grid.rowCount  # Total number of rows in the grid
    first_visible_row = 0

    try:
        while first_visible_row < total_rows:
            row_count = grid.visibleRowCount  # Get the number of currently visible rows

            # Read data from each visible row
            for row in range(first_visible_row, min(first_visible_row + row_count, total_rows)):
                row_data = tuple(grid.getCellValue(row, col).replace(".", ",") for col in columns)
                extracted_data.add(row_data)  # Add to set to avoid duplicates

            # Scroll down
            first_visible_row += row_count
            try:
                grid.firstVisibleRow = first_visible_row
                time.sleep(0.5)  # Wait for SAP to update the view
            except:
                break  # If scrolling is not possible, break the loop

        # Convert extracted data to clipboard-friendly format
        clipboard_data = "\n".join("\t".join(row) for row in extracted_data)
        pyperclip.copy(clipboard_data)  # Copy to clipboard

    except Exception as e:
        return f"Exception: {str(e)}"

    return f"{len(extracted_data)} rows copied from ZKBP1 transaction."


def zpp3u_va03_get_data(session):
    retrieved_data = dict()

    table = session.findById("wnd[1]/usr")

    for i in range(6, 10_000, 5):
        ord_field_id = partial_matching(session, rf"lbl\[0,{6}\]", id_root="wnd[1]/usr")
        creator_field_id = partial_matching(session, rf"lbl\[26,{7}\]", id_root="wnd[1]/usr")
        date_field_id = partial_matching(session, rf"lbl\[50,{9}\]", id_root="wnd[1]/usr")

        if ord_field_id and creator_field_id and date_field_id:
            # Get into VA03
            customer_ord_num = session.findById(ord_field_id).text
            creator = session.findById(creator_field_id).text
            doc_date = session.findById(date_field_id).text

            retrieved_data.setdefault("customer_order", []).append(customer_ord_num)
            retrieved_data.setdefault("creator", []).append(creator)
            retrieved_data.setdefault("doc_date", []).append(doc_date)

            table.verticalScrollbar.position = i - 1

        else:
            break

    return retrieved_data

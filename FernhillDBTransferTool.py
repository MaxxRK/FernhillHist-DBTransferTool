import pyodbc
import pandas as pd
import os
import logging
import warnings
import urllib.parse
import PySimpleGUI as sg
from psgtray import SystemTray
from sqlalchemy import create_engine, text
from configparser import ConfigParser
from tqdm import tqdm
from timeit import default_timer as timer
from datetime import datetime, time
import hashlib


def convert_time(sec):
    """
    Used to convert seconds number to Hours:Minutes:Seconds
  
    Parameters:
    arg1 (int): seconds for conversion
  
    Returns:
    string: Hour:Minute:Second
  
    """
    flSec = float(sec)
    min, flSec = divmod(flSec, 60)
    hour, min = divmod(min, 60)
    return "%d:%02d:%02d" % (hour, min, flSec)


def hist_data_index(fern_conn, db_engine):
    """
    Checks the historic data index[hdi] of both databases.
  
    Checks the hdi of fernhill against the hdi of the external database. Removes entries from external db that do not exist in Fernhill.
  
    Parameters:
    arg1 (pyodbc.conn): Connection object of pyodbc fernhill connection.
    arg2 (dbengine): Engine object from sqlAlchemy for external database.
  
    Returns:
    n/a
  
    """
    old_historics = []
    total_historics = []
    historic_tables = db_engine.execute("""SELECT table_name
                                              FROM information_schema.tables
                                              WHERE table_schema = 'public';"""
                                        ).fetchall()
    for row in historic_tables:
        table_name = row[0]
        old_historics.append(table_name)
    print("Fetching data to add to table")
    odbc_sql = """ SELECT SourceName, TableName, Units, MinScale, MaxScale, DataType FROM HistoricDataIndex """
    df_fern = pd.read_sql_query(odbc_sql, fern_conn)
    df_fern.to_sql('historicdataindex', db_engine, index=False, schema='public', if_exists='replace', method='multi', chunksize=1000)
    for indx in range(len(df_fern)):
        col2 = df_fern.at[indx, 'TableName']
        total_historics.append(col2.lower())
    for entry in old_historics:
        if entry not in total_historics and entry != 'historicdataindex':
            db_engine.execute(f"DROP TABLE {entry}")
            print(f"Dropping table that is no longer needed {entry}")
    print("Finished Historic Data Index Check!")


def hist_data(fern_conn, dbengine):
    """
    Drops external database tables and reinserts Fernhill matching table.
  
    Parameters:
    arg1 (pyodbc.conn): Connection object of pyodbc fernhill connection.
    arg2 (dbengine): Engine object from sqlAlchemy for external database.
  
    Returns:
    n/a
  
    """
    logger = logger_initialize()
    fern_curs = fern_conn.cursor()
    odbc_sql = """ SELECT TableName FROM HistoricDataIndex """
    hist_table = fern_curs.execute(odbc_sql).fetchall()
    row_count = len(hist_table)
    print("Running database operations...")
    logger.debug("Running database operations...")
    for row in tqdm(range(row_count)):
        try:
            hist_table_name = hist_table[row][0]
            logger.debug(f"Starting update for {hist_table_name}...")
            logger.debug("Running query for information to insert into PostgreSQL...")
            start = timer()
            df_fern = pd.read_sql_query(f"SELECT * FROM {hist_table_name}", fern_conn)
            end = timer()
            time_taken = round(end - start, 2)
            logger.debug(f"Fernhill query took {time_taken} to complete.")
            logger.debug("Starting External Database update.")
            start = timer()
            df_fern.to_sql('temptable', dbengine, index=False, schema='public', if_exists='replace', method='multi', chunksize=1000)
            dbengine.execute('ALTER TABLE temptable ADD COLUMN entryid SERIAL PRIMARY KEY;')
            end = timer()
            time_taken = round(end - start, 2)
            logger.debug(f"External Database operations took {time_taken} to complete.")
        except pyodbc.ProgrammingError:
            logger.error(" Historic table copy returned an error skipping...")


def table_transfer():
    """
    Reads config file and initializes connections to Fernhill and external database.
  
    Parameters:
    n/a
  
    Returns:
    n/a
  
    """
    
    root = os.path.abspath(os.path.dirname(__file__))
    config_path = os.path.join(root + '\config.ini')
    config = ConfigParser()
    config.read(config_path)
    # Connect to postgres server
    dbpassword = urllib.parse.quote_plus(config.get('database', 'Password'))

    dbengine = create_engine(
        config.get('database', 'Type') + "://" + config.get('database', 'Username') + ':' + 
        dbpassword + '@' + config.get('database', 'Host') + ':' + 
        config.get('database', 'Port') + '/' + config.get('database', 'DBName')
    )

    dbcnxn = dbengine.connect()
    print("PostgreSQL server information")
    record = dbcnxn.execute(text("SELECT version();"))
    for row in record:
        print(row)
    dbcnxn.close()

    # Connect to fernhill server
    fern_conn = pyodbc.connect(config.get('fernhill', 'ConnString'))
    fern_conn.autocommit = True

    hist_data_index(fern_conn, dbengine)
    start = timer()
    hist_data(fern_conn, dbengine)
    end = timer()
    time_taken = end - start
    time_taken = convert_time(time_taken)
    fern_conn.close()
    return time_taken


def config_handling(values, submit):
    """
    Reads config file and sets up settings layout based on whether the fields can be populated or not.
  
    Parameters:
    arg1 (dict): Values dictionary from window.read()
    arg2 (boolean): Whether the config file needs to be written or not.
  
    Returns:
    list: sg Settings Layout list.
  
    """
    root = os.path.abspath(os.path.dirname(__file__))
    config_path = os.path.join(root + '\config.ini')
    config = ConfigParser()
    if not os.path.isfile(config_path) and not submit:
        config.add_section('database')
        config.add_section('fernhill')
        config.add_section('misc')
        config.set('misc', 'autoupdate', 'True')
        with open(config_path, 'w') as f:
                config.write(f)
        settings_layout = [
            [sg.Text('Database', size=(55, 1), justification='center')],
            [sg.Push(), sg.Text('Type:'), sg.InputText()],
            [sg.Push(), sg.Text('User Name:'), sg.InputText()],
            [sg.Push(), sg.Text('Password:'), sg.InputText(password_char='*')],
            [sg.Push(), sg.Text('Host:'), sg.InputText()],
            [sg.Push(), sg.Text('Port:'), sg.InputText()],
            [sg.Push(), sg.Text('Database Name:'), sg.InputText()],
            [sg.Text('Fernhill', size=(55, 1), justification='center')],
            [sg.Push(), sg.Text('Connection String:'), sg.InputText()],
            [sg.Push(), sg.Text('Automatically Update?'), sg.Radio('Yes', "AutoUpdate", default=True, size=(10, 1), k='-AU1-'), sg.Radio('No', "AutoUpdate", default=False, size=(10, 1), k='-AU2-')],
            [sg.Push(), sg.Text('Auto Update Runs at 6:00am and 6:00pm.', size=(55, 1), justification='center')],
            [sg.Push(), sg.Text('YOU MUST HIT THE SUBMIT BUTTON TO SAVE CONFIG CHANGES', size=(55, 1), justification='center')],
            [sg.Button('Submit')]
        ]
        return config, settings_layout

    elif os.path.isfile(config_path) and not submit:
        root = os.path.abspath(os.path.dirname(__file__))
        config_path = os.path.join(root + '\config.ini')
        config.read(config_path)
        au1 = config.getboolean('misc', 'autoupdate')
        if not au1:
            au2 = True
        else:
            au2 = False
        sg.theme('dark grey 9')
        settings_layout = [
            [sg.Text('Database', size=(55, 1), justification='center')],
            [sg.Push(), sg.Text('Type:'), sg.InputText(config.get('database', 'type'))],
            [sg.Push(), sg.Text('User Name:'), sg.InputText(config.get('database', 'username'))],
            [sg.Push(), sg.Text('Password:'), sg.InputText(config.get('database', 'password'), password_char='*')],
            [sg.Push(), sg.Text('Host:'), sg.InputText(config.get('database', 'host'))],
            [sg.Push(), sg.Text('Port:'), sg.InputText(config.get('database', 'port'))],
            [sg.Push(), sg.Text('Database Name:'), sg.InputText(config.get('database', 'dbname'))],
            [sg.Text('Fernhill', size=(55, 1), justification='center')],
            [sg.Push(), sg.Text('Connection String:'), sg.InputText(config.get('fernhill', 'connstring'))],
            [sg.Push(), sg.Text('Automatically Update?'), sg.Radio('Yes', "AutoUpdate", default=au1, size=(10,1), k='-AU1-'), sg.Radio('No', "AutoUpdate", default=au2, size=(10,1), k='-AU2-')],
            [sg.Push(), sg.Text('Auto Update Runs at 6:00am and 6:00pm.', size=(55, 1), justification='center')],
            [sg.Push(), sg.Text('YOU MUST HIT THE SUBMIT BUTTON TO SAVE CONFIG CHANGES', size=(55, 1), justification='center')],
            [sg.Button('Submit', size=(25, 1), pad=((135, 0), (15, 15)))]
        ]
        return settings_layout
    else:
        config.read(config_path)
        config.set('database', 'Type', values[1])
        config.set('database', 'UserName', values[2])
        config.set('database', 'Password', values[3])
        config.set('database', 'Host', values[4])
        config.set('database', 'Port', values[5])
        config.set('database', 'DBName', values[6])
        config.set('fernhill', 'ConnString', values[7])
        if values["-AU1-"]:
            config.set('misc', 'autoupdate', 'True')
        else:
            config.set('misc', 'autoupdate', 'False')
        with open(config_path, 'w') as f:
            config.write(f)


def layouts(settings_layout):
    """
    Sets up the window layout.
  
    Parameters:
    arg1 (list): Settings Layout list for the settings tab.
  
    Returns:
    list: full window layout list for sg
  
    """
    sg.theme('dark grey 9')
    logging_layout = [
        [sg.Multiline(size=(65, 25), font='Courier 8', expand_x=True, expand_y=True, write_only=True,
                      reroute_stdout=True, reroute_stderr=True, echo_stdout_stderr=True, autoscroll=True, auto_refresh=True)],
        [sg.Button("Start Update"), sg.Button("Minimize to System Tray")]
    ]

    layout = [
        [sg.Text('Fernhill To Postgres Database Transfer Tool', size=(60, 2), justification='center', relief=sg.RELIEF_RIDGE, k='-TEXT HEADING-')]
    ]

    layout += [[sg.TabGroup([[
                sg.Tab('Terminal Output', logging_layout),
                sg.Tab('Settings', settings_layout),
                            ]])
               ]]
    return layout


def logger_initialize():
    """
   Sets up and controls the logger.
  
    Parameters:
    n/a
    
    Returns:
    object: logger object
  
    """
    logging.basicConfig(filename='dbtool.log',
                        filemode = 'w',
                        format = '%(levelname)s:%(asctime)s - %(message)s',
                        level = logging.ERROR)
    logger = logging.getLogger()
    root = os.path.abspath(os.path.dirname(__file__))
    log_path = os.path.join(root + '\dbtool.log')
    log_handler = logging.FileHandler(log_path)
    handler_format = logging.Formatter('%(levelname)s:%(asctime)s - %(message)s')
    log_handler.setFormatter(handler_format)
    logger.addHandler(log_handler)
    return logger



def main():
    """
    Main logic to run the gui window and call functions for autoupdate
  
    Parameters:
    n/a
  
    Returns:
    n/a
  
    """
    in_tray = False
    updating = False
    logger = logger_initialize()
    warnings.simplefilter(action='ignore', category=UserWarning)

    settings_layout = config_handling(values={}, submit=False)
    layout = layouts(settings_layout)
    window = sg.Window('Fernhill to SQL Database', layout, resizable=False )

    first = time(6, 0, 0)
    first_start = time(6, 1, 0)
    second = time(18, 0, 0)
    second_start = time(18, 1, 0)

    while True:
        event, values = window.read(timeout=100)
        # Check for auto update value is set to True and run at correct time
        if values["-AU1-"]:
            right_now = datetime.now().time()
            if right_now > first and right_now < first_start and not updating:
                print(f"Time and date is {datetime.now()}. SQL update is starting...")
                logger.debug(f"Time and date is {datetime.now()}. SQL update is starting...")
                window.perform_long_operation(table_transfer, '-UPDATE COMPLETE-')
                updating = True
            elif right_now > second and right_now < second_start and not updating:
                print(f"Time and date is {datetime.now()}. SQL update is starting...")
                logger.debug(f"Time and date is {datetime.now()}. SQL update is starting...")
                window.perform_long_operation(table_transfer, '-UPDATE COMPLETE-')
                updating = True

        # Check for input from the window
        if event == 'Submit':
            config_handling(values, submit=True)
            logger.debug('You entered', values)
        elif event == 'Start Update':
            print(f"Time and date is {datetime.now()}. SQL update is starting...")
            logger.debug(f"Time and date is {datetime.now()}. SQL update is starting...")
            window.perform_long_operation(table_transfer, '-UPDATE COMPLETE-')
            updating = True
        elif event == '-UPDATE COMPLETE-':
            print("All operations have completed!")
            updating = False

        # Check for user input to minimize to system tray
        elif event == 'Minimize to System Tray':
            menu = ['', ['Show Window', 'Exit']]
            tooltip = 'Fernhill to SQL Database Transfer Tool'
            sys_tray = SystemTray(menu, single_click_events=False, window=window, tooltip=tooltip, icon=sg.DEFAULT_BASE64_ICON )
            sys_tray.show_message('Fernhill to SQL Database Transfer Tool', 'Program still running in System Tray!')
            window.hide()
            sys_tray.show_icon()
            in_tray = True
            while in_tray:
                event, values = window.read(timeout=100)
                right_now = datetime.now().time()

                if event == sys_tray.key:
                    #sg.cprint(f'System Tray Event = ', values[event], c='white on red')
                    event = values[event]
                if values["-AU1-"]:
                    if right_now > first and right_now < first_start and not updating:
                        sys_tray.show_message('Starting 6:00am update!')
                        print(f"Time and date is {datetime.now()}. SQL update is starting...")
                        logger.debug(f"Time and date is {datetime.now()}. SQL update is starting...")
                        window.perform_long_operation(table_transfer, '-UPDATE COMPLETE-')
                        updating = True
                    elif right_now > second and right_now < second_start and not updating:
                        sys_tray.show_message('Starting 6:00pm update!')
                        print(f"Time and date is {datetime.now()}. SQL update is starting...")
                        logger.debug(f"Time and date is {datetime.now()}. SQL update is starting...")
                        window.perform_long_operation(table_transfer, '-UPDATE COMPLETE-')
                        updating = True
                if event == '-UPDATE COMPLETE-':
                    sys_tray.show_message("All operations have completed!")
                    print("All operations have completed!")
                    logger.debug("All operations have completed!")
                    updating = False    
                elif event in ('Show Window', '__DOUBLE_CLICKED__', sg.EVENT_SYSTEM_TRAY_ICON_DOUBLE_CLICKED):
                    window.un_hide()
                    window.bring_to_front()
                    sys_tray.close()
                    in_tray = False

        if event == sg.WIN_CLOSED or event == 'Exit':
            break

    sys_tray.close()
    window.close()

if __name__ == "__main__":
    # It asks for SQL Alchemy, but I cannot get it to work with Fernhill Yet. This is a WIP
    main()
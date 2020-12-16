import sqlite3
 

def scrub(thing):
    '''
    Description:
    ------------
    Scrub query inputs to prevent SQL injection attacks
    
    :param thing: Any - Object to be converted to a string and scrubbed
    
    :return: str - Scrubbed string to be used in a query
    '''
    
    thing = str(thing)
    new_thing = thing.replace(';', '')
    return new_thing.replace('--', '')

def scrub_columns(column_tuple):
    '''
    Description:
    ------------
    Scrub column name inputs to prevent SQL injection attacks
    
    :param column_tuple: tuple of strs - Column names
    
    :return: str - Scrubbed string to be used in a query
    '''
    
    columns = []
                
    for column in column_tuple:
        columns.append(scrub(column))
    
    return tuple(columns)

def compile_template(tuple_):
    '''
    Description:
    ------------
    Create table column template based on the number of columns given
    
    :param tuple_: tuple of strs - Column names
    
    :return: str - Table column template
    '''
    
    args  = '('
    args += '?, ' * len(tuple_)
    args  = args[:-2]
    args += ')'
    
    return args

 
class Database(object):
    get_all_tables = '''SELECT
                            name
                        FROM
                            sqlite_master
                        WHERE
                            type='table' AND
                            name NOT LIKE 'sqlite_%';'''
    get_structure = '''SELECT
                           sql 
                       FROM
                           sqlite_master
                       WHERE
                           type = 'table' AND
                           name = '{}';'''
    create_table_sql = '''CREATE TABLE IF NOT EXISTS
                              {} {};'''
    insert_sql = '''INSERT INTO
                        {} {}
                    VALUES
                        {};'''
    dump_sql = '''INSERT INTO {table}
                      SELECT {select} {union};'''
    
    def __init__(self, db_file):
        self.create_connection(db_file)
    
    def create_connection(self, db_file):
        '''
        Description:
        ------------
        Connect to/create database file for processing
        
        :param db_file: str - Full path to database file
        '''
        
        self.conn = None
        
        try:
            self.conn = sqlite3.connect(db_file)
            self.get_tables()
        except:
                import traceback
                traceback.print_exc()
    
    def get_tables(self):
        '''
        Description:
        ------------
        Determine the names of all tables within the database - accessable via
        `self.insides`
        '''
        
        self.insides = {}
        
        with self.conn:
            try:
                c = self.conn.cursor()
                c.execute(self.get_all_tables)
                
                tables = c.fetchall()
                
                for table in tables:
                    self.insides[table[0]] = []
                
                self.get_columns()
            except:
                import traceback
                traceback.print_exc()
    
    def get_columns(self):
        '''
        Description:
        ------------
        Determine the names of all columns in a given table within the database
        - accessable via `self.insides[table]`
        '''
        
        with self.conn:
            try:
                for table in self.insides.keys():
                    c = self.conn.cursor()
                    c.execute(self.get_structure.format(scrub(table)))
                    
                    schema = c.fetchall()[0][0]
                    schema = schema.split('(')[1].split(')')[0].split(',')
                    
                    columns = []
                    for scheme in schema:
                        columns.append(scheme.strip().split()[0].replace("'", ''))
                    
                    self.insides[table] = tuple(columns)
            except:
                import traceback
                traceback.print_exc()
        
    def create_table(self, table_name, column_tuple):
        '''
        Description:
        ------------
        Creates a table with the given name (if such a table doesn't already
        exists) and given column names
        
        :param table_name:   str           - Name of table to create
        :param column_tuple: tuple of strs - Column names of the new table
        '''
        
        with self.conn:
            try:
                c = self.conn.cursor()
                
                c.execute(self.create_table_sql.format(scrub(table_name),
                                                       scrub_columns(column_tuple)).replace(',)', ')'))
                self.get_tables()
            except:
                import traceback
                traceback.print_exc()
    
    def insert(self, table_name, data_tuple, column_tuple=None):
        '''
        Description:
        ------------
        Creates a row of data within a given table
        
        :param table_name:   str              - Name of table to insert into
        :param data_tuple:   iterable of any  - Data to insert
        :param column_tuple: tuple of strs    - Column names for data to insert
        
        :return: bool - Whether or not the operation was successful
        '''
        
        data = tuple([scrub(datum) for datum in data_tuple])
        
        if table_name not in self.insides.keys():
            if column_tuple:
                self.create_table(table_name, column_tuple)
            else:
                return False
        
        if not column_tuple:
            column_tuple = self.insides[table_name]
        
        with self.conn:
            try:
                command = self.insert_sql.format(scrub(table_name),
                                                 scrub_columns(column_tuple),
                                                 compile_template(data_tuple).replace(',)', ')'))
                c = self.conn.cursor()
                c.execute(command, data)
            except:
                import traceback
                traceback.print_exc()
                return False
        return True
    
    def dump(self, table_name, data_pile, column_tuple=None):
        '''
        Description:
        ------------
        Creates multiple rows of data within a given table simultaneously
        
        :param table_name:   str                          - Name of table to insert into
        :param data_pile:    iterable of iterables of any - Data to insert
        :param column_tuple: tuple of strs                - Column names for data to insert
        '''
        
        flattened_pile = []
        for row in data_pile:
            for datum in row:
                flattened_pile.append(scrub(datum))
        flattened_pile = tuple(flattened_pile)
        
        if table_name not in self.insides.keys():
            if column_tuple:
                self.create_table(table_name, column_tuple)
            else:
                return False
        
        if not column_tuple:
            column_tuple = self.insides[table_name]
        
        select_sql = []
        for column in column_tuple:
            select_sql.append('(?) AS {}'.format(scrub(column)))
        select_sql = ', '.join(select_sql)
        
        union_sql = ''
        for i in range(len(data_pile) - 1):
            union_sql += '\nUNION ALL SELECT {}'.format(', '.join(('(?),' * len(column_tuple)).split(',')[:-1]))
        
        dump_sql_formatted = self.dump_sql.format(table=scrub(table_name),
                                                  select=scrub(select_sql),
                                                  union=scrub(union_sql))
        with self.conn:
            try:
                c = self.conn.cursor()
                c.execute(dump_sql_formatted, flattened_pile)
            except:
                import traceback
                traceback.print_exc()
                return False
        return True
     
    def select_all(self, table_name):
        '''
        Description:
        ------------
        Retrieve all rows from a given table
        
        :param table_name: str - Name of table to select from
        
        :return result: list of tuples - Data rows retrieved from the given table
        '''
        
        with self.conn:
            try:
                if table_name in self.insides.keys():
                    c = self.conn.cursor()
                    c.execute('SELECT * FROM {};'.format(scrub(table_name)))
                 
                    result = c.fetchall()
                    
                    if result:
                        return result
                    return []
                    
                return []
            except:
                import traceback
                traceback.print_exc()
                return []
 
    def select(self, table_name, columns=None, condition=None):
        '''
        Description:
        ------------
        Retrieve specified column(s) data from a given table based on a user
        specified condition (if given)
        
        :param table_name: str      - Name of table to select from
        :param columns:    iterable - Names of columns to select data from
        :param condition:  str      - SQL conditional string to qualify rows
                                      for selection
        
        :return result: list of tuples - Data rows retrieved from the given table
        '''
        
        if not columns:
            self.get_tables()
            
            if table_name in self.insides.keys():
                columns = []
                
                for column in self.insides[table_name]:
                    columns.append(column)
            else:
                raise Exception('Table not found in db')
        
        if (not type(columns) == list) and (not type(columns) == tuple):
            columns = [columns]
        
        query = 'SELECT {} FROM {}'
        
        with self.conn:
            try:
                if table_name in self.insides.keys():
                    c = self.conn.cursor()
                    
                    if condition:
                        query += ' WHERE {};'
                        c.execute(query.format(scrub(', '.join(columns)),
                                               scrub(table_name),
                                               scrub(condition)).replace(',)', ')'))
                    else:
                        query += ';'
                        c.execute(query.format(scrub(', '.join(columns)),
                                               scrub(table_name)).replace(',)', ')'))
                    result = c.fetchall()
                    
                    if result:
                        return result
                    return []
                    
                else:
                    return []
            except:
                import traceback
                traceback.print_exc()
                return []
    
    def delete_all(self, table_name):
        '''
        Description:
        ------------
        Delete all rows in a given table
        
        :param table_name: str - Name of table to delete from
        
        :return: bool - Whether or not the operation was successful
        '''
        
        with self.conn:
            try:
                if table_name in self.insides.keys():
                    c = self.conn.cursor()
                    c.execute('DELETE FROM {};'.format(scrub(table_name)))
                 
                    return True
                return False
            except:
                import traceback
                traceback.print_exc()
                return False
    
    def delete(self, table_name, condition=None):
        '''
        Description:
        ------------
        Delete rows in a given table based on the given condition (if no
        condition is specified, all rows will be deleted)
        
        :param table_name: str - Name of table to delete from
        :param condition:  str - SQL conditional string to qualify rows for deletion
        
        :return: bool - Whether or not the operation was successful
        '''
        
        if condition:
            query = 'DELETE FROM {} WHERE {};'
        else:
            query = 'DELETE FROM {}'
        
        with self.conn:
            try:
                if table_name in self.insides.keys():
                    c = self.conn.cursor()
                    
                    if condition:
                        c.execute(query.format(scrub(table_name),
                                               scrub(condition)))
                    else:
                        query += ';'
                        c.execute(query.format(scrub(table_name)))
                        
                    return True
                else:
                    return False
            except:
                import traceback
                traceback.print_exc()
                return False
    

import sqlite3
from sqlite3 import Error
 

def scrub(thing):
    return ''.join(chr for chr in thing if chr not in [';', '-'])

def scrub_columns(column_tuple):
    columns = []
                
    for column in column_tuple:
        columns.append(scrub(column))
    
    return tuple(columns)

def compile_template(tuple_):
    args = '('
    args += '?, ' * len(tuple_)
    args = list(args)
    args = ''.join(args[:-2])
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
    
    def __init__(self, db_file):
        self.create_connection(db_file)
    
    def create_connection(self, db_file):
        '''
        TODO
        '''
        
        self.conn = None
        
        try:
            self.conn = sqlite3.connect(db_file)
            self.get_tables()
        except Error as e:
            print(e)
    
    def get_tables(self):
        '''
        TODO
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
            except Error as e:
                print(e)
    
    def get_columns(self):
        '''
        TODO
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
            except Error as e:
                print(e)
        
    def create_table(self, table_name, column_tuple):
        '''
        TODO
        '''
        
        with self.conn:
            try:
                c = self.conn.cursor()
                c.execute(self.create_table_sql.format(scrub(table_name),
                                                       scrub_columns(column_tuple)))
                
                self.get_tables()
            except Error as e:
                print(e)
    
    def insert(self, table_name, data_tuple, column_tuple=None):
        '''
        TODO
        '''
        
        if table_name not in self.insides.keys():
            if column_tuple:
                self.create_table(table_name, column_tuple)
            else:
                return False
        
        if not column_tuple:
            column_tuple = self.insides[table_name]
        
        with self.conn:
            try:
                c = self.conn.cursor()
                c.execute(self.insert_sql.format(scrub(table_name),
                                                 scrub_columns(column_tuple),
                                                 compile_template(data_tuple)), data_tuple)
            except Error as e:
                print(e)
        return True
     
    def select_all(self, table_name):
        '''
        TODO
        '''
        
        with self.conn:
            try:
                if table_name in self.insides.keys():
                    c = self.conn.cursor()
                    c.execute('SELECT * FROM {};'.format(scrub(table_name)))
                 
                    return c.fetchall()
                return False
            except Error as e:
                print(e)
                return False
 
    def select(self, table_name, columns=None, condition=None):
        '''
        TODO
        '''
        
        if not columns:
            columns = ['*']
        
        if (not type(columns) == list) or (not type(columns) == tuple):
            columns = [columns]
        
        query = 'SELECT {} FROM {}'
        
        with self.conn:
            try:
                if table_name in self.insides.keys():
                    c = self.conn.cursor()
                    
                    if condition:
                        query += ' WHERE {}'
                        c.execute(query.format(scrub(', '.join(columns)),
                                               scrub(table_name),
                                               scrub(condition)))
                    else:
                        c.execute(query.format(scrub(', '.join(columns)),
                                               scrub(table_name)))
                    return c.fetchall()
                else:
                    return False
            except Error as e:
                print(e)
                return False
    
 
if __name__ == '__main__':
    my_db = Database('pythonsqlite.db')
    my_db.create_table('hi', ('title', 'body'))
    
    my_db.insert('hi', (1, 4))
    my_db.insert('bi', ('hmm', '["ouch", "oof"]'), ('title', 'body'))
    
    print(my_db.select_all('hi'))
    print(' ')
    print(my_db.select_all('bi'))
    
    
    
    
import sqlite3
import pandas as pd
from abc import ABC, abstractmethod


class AbstractTask(ABC):

    @abstractmethod
    def execute(self, **kwargs) -> pd.DataFrame:
        pass


class Excel(AbstractTask):
    def __init__(self, file: str):
        self.__file = file

    def execute(self, **kwargs) -> pd.DataFrame:
        df = pd.read_excel(io=self.__file, header=[0, 1, 2])
        f1 = pd.DataFrame(df[['id', 'company']])
        f1.columns = ['id', 'company']
        f2 = pd.DataFrame(df[['fact', 'forecast']])
        f2 = f2.T
        f2.columns = f1
        f2 = f2.stack()
        f3 = f2.index.to_frame(index=False)
        col = list(zip(*f3[3].values))
        f3 = pd.DataFrame(f3.drop(columns=[3]))
        f3.columns = ['type', 'q', 'data']
        f3['id'] = col[0]
        f3['company'] = col[1]
        f3['value'] = f2.values
        return f3


class AddDate(AbstractTask):
    def __init__(self, f: pd.DataFrame, dt: str):
        self.__f = f
        self.__dt = dt

    def execute(self, **kwargs) -> pd.DataFrame:
        f = self.__f
        f['dt'] = pd.date_range(start=self.__dt, periods=self.__f.shape[0], freq='D')
        return f


class Sqlite3(AbstractTask):
    def connect(self):
        return sqlite3.connect('ursib.db')

    def execute(self, **kwargs) -> pd.DataFrame:
        with self.connect() as conn:
            cur = conn.cursor()
            res = cur.execute(kwargs.get('query'))
            return pd.DataFrame(res.fetchall())


class Sqlite3Many(Sqlite3):

    def execute(self, **kwargs) -> pd.DataFrame:
        with self.connect() as conn:
            cur = conn.cursor()
            res = cur.executemany(kwargs.get('query'), kwargs.get('params'))
            return pd.DataFrame(res.fetchall())


excel = Excel('data.xlsx')
data = excel.execute()
add_date = AddDate(data, '2023-01-01')
data = add_date.execute()
# print(data)

sql = Sqlite3()
sql_m = Sqlite3Many()

q1 = '''
    create table if not exists my_table (
        type text, 
        q text, 
        data text, 
        id integer, 
        company text, 
        value integer, 
        date date,
        primary key (type, q, data, id, company)
    )'''
sql.execute(query=q1)
sql.execute(query='delete from my_table')

q2 = 'insert into my_table (type, q, data, id, company, value, date) values (?, ?, ?, ?, ?, ?, ?)'
params = [tuple(v.strftime('%Y-%m-%d') if isinstance(v, pd.Timestamp) else v for k, v in x.items()) for x in data.to_dict('records')]
sql_m.execute(query=q2, params=params)

d3 = sql.execute(query='select q, sum(value) value from my_table group by q')
print(20*'=')
print(d3)

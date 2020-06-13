from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, REAL
from datetime import datetime, timedelta

Base = declarative_base()

class Price(Base):
    __tablename__ = 'prices'

    time = Column(String(30), primary_key=True)
    nav = Column(REAL)
    sell1 = Column(Integer)
    sell1_cnt = Column(Integer)
    sell2 = Column(Integer)
    sell2_cnt = Column(Integer)
    buy1 = Column(Integer)
    buy1_cnt = Column(Integer)
    buy2 = Column(Integer)
    buy2_cnt = Column(Integer)

    def __repr__(self):
        return "<Price('%s', '%.2f', '%d', '%d')>" % (self.time, self.nav, self.sell1, self.buy1)

    def date_time(self):
        dt = datetime.strptime(self.time, '%Y-%m-%d %H:%M:%S.%f')
        if dt.hour >= 1 and dt.hour <= 7:
            dt += timedelta(hours=12)
        return dt
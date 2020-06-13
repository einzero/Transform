import copy

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import make_transient
from price import Price

import os

def run(key):
    engine = create_engine('sqlite:///{0}.sqlite'.format(key), echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    prices_by_date = {}
    prices = []
    cur_date = None
    for instance in session.query(Price):
        time = instance.date_time()
        date = time.date()

        if cur_date is None:
            cur_date = date
        elif cur_date != date:
            prices_by_date[cur_date] = prices
            prices = []
            cur_date = date

        session.expunge(instance)
        make_transient(instance)
        instance.time = time.strftime('%Y-%m-%d %H:%M:%S.%f')
        prices.append(instance)

    engine.dispose()

    if len(prices) > 0:
        prices_by_date[cur_date] = prices

    if not os.path.isdir(key):
        os.mkdir(key)

    for k,v in prices_by_date.items():
        engine = create_engine('sqlite:///{0}/{1}.db'.format(key, k), echo=True)
        Session = sessionmaker(bind=engine)
        session = Session()
        Price.metadata.create_all(engine)

        session.add_all(v)
        session.commit()
        engine.dispose()

if __name__ == '__main__':
    keys = ['251340', '069500', '102110', '114800', '148020', '229200', '232080']
    for key in keys: run(key)
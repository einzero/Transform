import copy

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from price import Price
from datetime import datetime
import os

def run(key):
    engine = create_engine('sqlite:///{0}.sqlite'.format(key), echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    prices_by_date = {}
    prices = []
    cur_date = None
    for instance in session.query(Price).limit(2000):
        time = instance.date_time()
        date = time.date()

        if cur_date is None:
            cur_date = date
        elif cur_date is not date:
            prices_by_date[cur_date] = prices
            prices = []
            cur_date = date

        new_inst = copy.deepcopy(instance)
        new_inst.time = str(time)
        prices.append(new_inst)

    if len(prices) > 0:
        prices_by_date[cur_date] = prices

    if not os.path.isdir(key):
        os.mkdir(key)

    for k,v in prices_by_date.items():
        engine = create_engine('sqlite:///{0}/{1}.db'.format(key, k), echo=True)
        Session = sessionmaker(bind=engine)
        session = Session()
        Price.metadata.create_all(engine)

        for inst in v:
            print(len(prices_by_date.keys()))
            #session.add(inst)
        session.commit()



if __name__ == '__main__':
    run('251340')
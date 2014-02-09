import sys
import time
import datetime
from sqlalchemy import distinct
import requests
import datetime 
from sqlalchemy import Column, Integer, String, ForeignKey, Float, Date, PrimaryKeyConstraint
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, mapper, relation, sessionmaker

from pprint import pprint
import math

Base = declarative_base()

engine = create_engine("postgresql+psycopg2://poweruser1:h6o3W1a5R5d@success-depot.cwgfpgjh6ucc.us-west-2.rds.amazonaws.com/sd_staging")

class Results(Base):
	__tablename__ = "results"
	account_id = Column(String, index=True)
	account_name = Column(String)
	month = Column(Integer)
	year = Column(Integer)
	exp_winning = Column(Integer)
	exp_winning_no_engagement = Column(Integer)
	experiments = Column(Integer)
	percent_winning = Column(Integer)
	__table_args__ = (PrimaryKeyConstraint('month', 'year', 'account_id'),)

metadata = Base.metadata
metadata.create_all(engine)

Session = sessionmaker(bind=engine)
sdstaging = Session()

r = Results()
# r.account_id = "0"
# r.month = 1
# r.year = 1 
# sdstaging.merge(r)
sdstaging.commit()


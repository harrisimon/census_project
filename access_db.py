import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
load_dotenv()
url = os.environ['DATABASE_URL']
engine = create_engine(url)
query = 'select * from "state_populations" WHERE "state"="CA"'

# df = pd.read_sql_query(text('select * from "state_populations"'), con=engine)
pop_df = pd.DataFrame(engine.connect().execute(text(query)))
print(pop_df.head())
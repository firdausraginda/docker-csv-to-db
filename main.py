
from dotenv import load_dotenv
import os

print(load_dotenv())
# DB_POSTGRES_CONN = {"PORT": "", "HOST": "", "POSTGRES_DB": "",
#                     "POSTGRES_PASSWORD": "", "POSTGRES_USER": ""}
# [DB_POSTGRES_CONN.update({key:os.environ[key]}) for key in DB_POSTGRES_CONN.keys()]
# print(DB_POSTGRES_CONN)
# site.update({'Author':'Sammy Shark'})

# for key in DB_POSTGRES_CONN.keys():
    # print(os.environ[key])
    # print({key:os.environ[key]})



# POSTGRES_DB=os.getenv("POSTGRES_DB")
# print(POSTGRES_DB)
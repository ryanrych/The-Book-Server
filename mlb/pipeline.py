import os

from dotenv import load_dotenv


load_dotenv()
host = os.getenv("HOST")
port = os.getenv("PORT")
user = os.getenv("USER")
password = os.getenv("PASSWORD")



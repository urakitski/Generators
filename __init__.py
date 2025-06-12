from clients_generator import get_client_info
from fastavro import writer, parse_schema
from datetime import datetime
import numpy as np
import pandas as pd

clients_df = get_client_info()

print(clients_df.head(20))
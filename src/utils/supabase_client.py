"""
This file handles the connection to the supabase database.
"""

import os
from dotenv import load_dotenv
from supabase import create_client

# region ------------ Load env variables ------------
load_dotenv()
# endregion


# region ------------ Supabase client ------------
SUPABASE_URL: str = os.getenv("SUPABASE_URL")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# endregion

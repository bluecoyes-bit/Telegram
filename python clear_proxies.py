# clear_proxies.py
import asyncio
from database import SuiteDatabase

async def clear_cached_proxies():
    db = SuiteDatabase()
    # Unset the 'proxy' field for all accounts in the database
    result = db.src_accounts.update_many({}, {"$unset": {"proxy": ""}})
    print(f"✅ Successfully cleared cached proxies from {result.modified_count} accounts.")
    print("🚀 You can now delete this script and restart your bot.")

if __name__ == "__main__":
    asyncio.run(clear_cached_proxies())
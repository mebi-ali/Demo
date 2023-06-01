import requests
import os
import sys
sys.path.insert(0, '../')
from fast.main import get_data
async def main():
    data = await get_data()
    print(type(data))

# Run the main function
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
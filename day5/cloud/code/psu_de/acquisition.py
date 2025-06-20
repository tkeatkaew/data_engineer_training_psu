
import time
import datetime
import requests
import os
import asyncio

import models

async def get_pm_2_5_data(url, started_datetime) -> list[datetime.datetime, float]:

    url = url + f"&started_datetime={started_datetime.isoformat().replace('+00:00', '')}"
    print(url)
    response = requests.get(url)

    if response.status_code != 200:
        return []


    json_resonse = response.json()
    data = json_resonse['sensors']['pm_2_5']['data']
    device_id = json_resonse['device_id']

    if len(data) == 0:
        return []

    return [device_id, datetime.datetime.fromisoformat(data[-1][0]), data[-1][1]]


async def run(airthai_url):
    while True:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        started_datetime = now - datetime.timedelta(minutes=1)

        print('weakup and run at', now)
        value = await   get_pm_2_5_data(airthai_url, started_datetime)

        if value:
            device_id, ts, pm_2_5_value = value
            print(f"Saving data: {ts} - {pm_2_5_value}")
            await models.save_to_database(ts, pm_2_5_value, dict(device_id=device_id))


        time.sleep(60)



async def main():

    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/environment")
    airthai_url = os.getenv("AIRTHAI_URL")

    if not airthai_url:
        print("Please set the AIRTHAI_URL environment variable.")
        return

    await models.initialize_beanie(mongodb_uri)

    await run(airthai_url)




if __name__ == "__main__":
    asyncio.run(main())
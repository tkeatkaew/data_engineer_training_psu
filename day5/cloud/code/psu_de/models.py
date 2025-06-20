import beanie
import datetime

from motor.motor_asyncio import AsyncIOMotorClient

class SensorData(beanie.Document):
    ts: datetime.datetime 
    meta: dict
    value: float

    class Settings:
        timeseries = beanie.TimeSeriesConfig(
            time_field="ts", #  Required
            meta_field="meta", #  Optional
        )


async def initialize_beanie(mongodb_uri: str):

    client = AsyncIOMotorClient(mongodb_uri)
    database = client["environmentdb"]
    await beanie.init_beanie(database, document_models=[SensorData])



async def save_to_database(ts: datetime.datetime, value: float, meta: dict = {}):
    sensor_data = SensorData(ts=ts, meta=meta, value=value)
    await sensor_data.insert()

async def get_data():
    ts = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(minutes=60)
    data = await SensorData.find(SensorData.ts >= ts).to_list()
    return data

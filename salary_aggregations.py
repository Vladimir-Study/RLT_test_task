from motor.motor_asyncio import AsyncIOMotorClient
from envparse import env
from datetime import datetime
import pandas as pd


env.read_envfile()
MONGODB_URL = env("MONGODB_URL", default="not found")


client = AsyncIOMotorClient(MONGODB_URL).admin.initial_data


class Aggregate:
    def __init__(self, date_from: str, date_to: str, group_type: str):
        self.date_from = Aggregate.convert_dt_from_str(date_from)
        self.date_to = Aggregate.convert_dt_from_str(date_to)
        self.group_type = group_type

    @staticmethod
    def convert_dt_from_str(convert_date: str) -> datetime:
        """
        function for conver from string to datetime
        """
        return_date = datetime.strptime(convert_date, "%Y-%m-%dT%H:%M:%S")
        return return_date

    async def filters_data(self) -> list[dict]:
        """
        filters data on the date
        """
        try:
            return_data_list = await client.find().to_list(None)
            return_data_list = list(
                filter(
                    lambda line: self.date_from < line.get(
                        "dt") < self.date_to,
                    return_data_list,
                )
            )
            return return_data_list
        except Exception as E:
            print(f"Error: {E}")

    async def convert_to_dataframe(self, filter_data: list[dict]) -> pd.DataFrame:
        """
        converting list of data to Dataframe pandas
        """
        return pd.DataFrame(filter_data).drop("_id", axis=1)

    async def group_by_type(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        grouping according to the specified parameters
        """
        freq_dict = {"hour": "h", "day": "D", "week": "W", "month": "MS"}
        df_data = data_frame.groupby(
            pd.Grouper(key="dt", axis=0, freq=freq_dict[self.group_type])
        )
        return df_data.sum()


class Formatting:
    def __init__(self, format_dt: pd.DataFrame):
        self.format_dt = format_dt

    async def return_format(self):
        return_data = {"dataset": [], "labels": []}
        for index in self.format_dt.index:
            return_data["dataset"].append(self.format_dt["value"][index])
            return_data["labels"].append(index.strftime("%Y-%m-%dT%H:%M:%S"))
        return return_data


async def get_payment_data(insert_data: dict) -> dict:
    tests = Aggregate(
        insert_data["dt_from"], insert_data["dt_upto"], insert_data["group_type"]
    )
    filters_data = await tests.filters_data()
    df_data = await tests.convert_to_dataframe(filters_data)
    df_data = await tests.group_by_type(df_data)
    formatting = Formatting(df_data)
    return await formatting.return_format()

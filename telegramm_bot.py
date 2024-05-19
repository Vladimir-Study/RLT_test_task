import asyncio
import json
from telebot.async_telebot import AsyncTeleBot
from envparse import env
from salary_aggregations import get_payment_data


env.read_envfile()
BOT_TOKEN = env("BOT_TOKEN", default="env data not found")


bot = AsyncTeleBot(BOT_TOKEN)


@bot.message_handler()
async def send_hello(message):
    try:
        insert_data = json.loads(message.text)
        return_data_message = await get_payment_data(insert_data)
        await bot.send_message(
            message.chat.id,
            return_data_message,
        )
    except Exception as E:
        await bot.reply_to(message, E)


if __name__ == "__main__":
    asyncio.run(bot.polling())

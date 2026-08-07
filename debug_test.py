import asyncio
from pyrogram import Client, filters

app = Client(
    "debug_test",
    api_id=37934507,
    api_hash="ae0b733927221df2c6c5ccd0060d0dd6",
    bot_token="8854336055:AAF_DaDLVftq9BX_wFHnZ1ZBuRBB-TrgbJk",
)


@app.on_message(filters.private)
async def echo(client, message):
    print(f"GOT MESSAGE: {message.text}")
    await message.reply_text("pong")


app.run()

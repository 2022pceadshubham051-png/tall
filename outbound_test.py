import asyncio
from pyrogram import Client

app = Client(
    "outbound_test",
    api_id=37934507,
    api_hash="ae0b733927221df2c6c5ccd0060d0dd6",
    bot_token="8854336055:AAF_DaDLVftq9BX_wFHnZ1ZBuRBB-TrgbJk",
)


async def main():
    await app.start()
    print("Started. Trying to send a message to OWNER_ID...")
    try:
        await app.send_message(8644197194, "Outbound test — if you see this, sending works fine.")
        print("SEND SUCCEEDED")
    except Exception as e:
        print("SEND FAILED:", repr(e))
    await app.stop()


asyncio.run(main())

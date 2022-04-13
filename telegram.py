import json
import logging
from telethon import TelegramClient
from telethon.tl import functions
from telethon.errors.rpcerrorlist import ChannelPrivateError
from telethon.tl.functions.messages import GetHistoryRequest

# Configure logging
logging.basicConfig(filename="log.log", level=logging.DEBUG)

_client = None


def initialize_client():
    """Initialize Telegram client using the credentials given in config.json."""
    global _client
    if _client is None:
        with open("config.json", "r") as file:
            data = file.read()
        config = json.loads(data)
        api_id = config["credentials"]["api_id"]
        api_hash = config["credentials"]["api_hash"]
        if api_id != "" and api_hash != "":
            _client = TelegramClient("session", api_id, api_hash)
        else:
            raise Exception(
                "Please set your api_id and api_hash in config.json. More information "
                "can be found at https://core.telegram.org/api/obtaining_api_id."
            )


async def get_client():
    if _client is not None:
        return _client
    else:
        initialize_client()
        return _client


# Call the API once to fetch 100 messages
async def fetch_messages(chat, size=100, offset_id=0, max_id=0, min_id=0):
    if _client is None:
        initialize_client()
    async with _client as client:
        try:
            history = await client(
                GetHistoryRequest(
                    peer=chat,
                    limit=size,  # 100 is the maximum number of messages that can
                    # be retrieved per request
                    offset_date=None,
                    offset_id=offset_id,
                    max_id=max_id,
                    min_id=min_id,
                    add_offset=0,
                    hash=0,
                )
            )
        except ChannelPrivateError:
            print("Chat", chat, "is private")
    return history.messages


async def fetch_message(chat, message_id):
    if _client is None:
        initialize_client()
    async with _client as client:
        return await client.get_messages(chat, ids=message_id)


async def get_chat_info(chat):
    if _client is None:
        initialize_client()
    async with _client as client:
        data = await client(functions.channels.GetFullChannelRequest(channel=chat))
        json_data = data.to_json()
    return json.loads(json_data)


async def is_private(chat):
    if _client is None:
        initialize_client()
    async with _client as client:
        result = await client.get_entity(chat).restricted
    return result


async def get_chat_name(chat_id):
    return await get_chat_info(chat_id)["chats"][0]["title"]


async def get_chat_ids(chat_names):
    return [await get_chat_metadata(chat_name)["id"] for chat_name in chat_names]


async def get_chat_metadata(chat):
    """
    Get meta information about the given chat.

    chat - id or username of the chat
    """
    chat_info = await get_chat_info(chat)
    type = "broadcast"
    if chat_info["chats"][0]["megagroup"] is True:
        type = "megagroup"
    if chat_info["chats"][0]["gigagroup"] is True:
        type = "gigagroup"
    can_comment = 1
    if type == "broadcast":
        can_comment = 0 if len(chat_info["chats"]) == 1 else 1
    metadata = {
        "id": chat_info["chats"][0]["id"],
        "name": chat_info["chats"][0]["title"],
        "username": chat_info["chats"][0]["username"],
        "type": type,
        "can_comment": can_comment,
    }
    return metadata


# Try to join the chat
async def join_chat(chat):
    if _client is None:
        initialize_client()
    print("Joining", await get_chat_info(chat)["chats"][0]["username"])
    try:
        async with _client as client:
            await client(functions.channels.JoinChannelRequest(channel=chat))
    except Exception as e:
        print("Failed to join chat:", e)


# Leave the chat
async def leave_chat(chat):
    if _client is None:
        initialize_client()
    print("Leaving", get_chat_info(chat)["chats"][0]["username"])
    try:
        async with _client as client:
            await client(functions.channels.LeaveChannelRequest(channel=chat))
    except Exception as e:
        print("Failed to join chat:", e)


async def print_user_dialogs():
    """
    Print the name and id of all chats of the user whose credentials are used.
    Being able to access these personal chats might be useful for testing.
    """
    if _client is None:
        initialize_client()
    async with _client as client:
        async for dialog in client.iter_dialogs():
            print(dialog.name, dialog.entity.id)


async def send_message(chat_id, message):
    if _client is None:
        initialize_client()
    async with _client as client:
        await client.send_message(chat_id, message)

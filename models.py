import hashlib
from peewee import (
    DateTimeField,
    DoesNotExist,
    IntegerField,
    Model,
    TextField,
    BooleanField,
    ForeignKeyField,
    SqliteDatabase,
)
from telethon.errors.rpcerrorlist import ChannelPrivateError
from telethon.tl.types import MessageMediaWebPage, WebPageEmpty
from urllib.parse import urlparse

# own modules
import telegram
from utility_functions import get_current_datetime, load_json


db = SqliteDatabase("data/sqlite.db")


class BaseTable(Model):
    class Meta:
        database = db


class TelegramChat(BaseTable):
    id = IntegerField(primary_key=True)
    name = TextField()
    username = TextField()
    type = TextField()
    can_comment = BooleanField()
    added = DateTimeField(default=get_current_datetime)

    @classmethod
    async def get_or_create_by_id(cls, chat_id):
        try:
            return cls.get_by_id(chat_id)
        except DoesNotExist:
            # Get chat meta data
            try:
                chat_metadata = await telegram.get_chat_metadata(chat_id)
            except ValueError:
                print(
                    f"ValueError in chat {str(chat_id)}. This probably means that the "
                    "chat is not known by its id yet. You need to first retrieve it "
                    "in some other way. If you know the username, use "
                    "add_chat_by_username instead. See "
                    "https://docs.telethon.dev/en/latest/concepts/entities.html#summary"
                    " for more information."
                )
                return
            except ChannelPrivateError:
                print(
                    "The chat",
                    chat_id,
                    "could not be added to the database because it is private.",
                )
                return
            # Create, store and return TelegramChat instance
            chat = cls(
                id=chat_id,
                name=chat_metadata["name"],
                username=chat_metadata["username"],
                type=chat_metadata["type"],
                can_comment=chat_metadata["can_comment"],
            )
            chat.save(force_insert=True)
            print(chat, "saved")
            return chat


# Not to be confused with the Telethon Message type
class TelegramMessage(BaseTable):
    chat = ForeignKeyField(TelegramChat)
    message_id = IntegerField()
    content = TextField()
    url = TextField(null=True)
    hash = TextField(primary_key=True)
    creation_date = DateTimeField()
    last_retrieved = DateTimeField()
    views = IntegerField(null=True)
    forwards = IntegerField(null=True)

    def __str__(self):
        return (
            f"TelegramMessage with chat_id {self.chat.id} and message_id "
            f"{self.message_id} has the following content:\n{self.content}"
        )

    @classmethod
    async def from_telethon_message(
        cls, telethon_message, store_forwarded_original=False
    ):
        """
        Extract relevant data from the given Telethon message object and return a
        TelegramMessage object containing this data.

        Args:
            telethon_message (telethon.tl.patched.Message): The message object to be
                converted
            store_forwarded_original (bool, optional): Whether to store the original
                metadata for forwarded messages (original chat id, message id and date).
                Defaults to False.

        Returns:
            TelegramMessage: Message object of our type, containing the relevant data.
        """
        if (
            telethon_message.fwd_from
            and telethon_message.fwd_from.from_id
            and telethon_message.fwd_from.channel_post is not None
            and store_forwarded_original is True
        ):
            chat_id = telethon_message.fwd_from.from_id.channel_id
            message_id = telethon_message.fwd_from.channel_post
            creation_date = telethon_message.fwd_from.date
        else:
            chat_id = telethon_message.peer_id.channel_id
            message_id = telethon_message.id
            creation_date = telethon_message.date
        content = telethon_message.message

        # If message links to website, store base url
        url = None
        if (
            telethon_message.media
            and type(telethon_message.media) == MessageMediaWebPage
            and type(telethon_message.media.webpage) != WebPageEmpty
        ):
            # Convert to normalized base url
            base_url = urlparse(telethon_message.media.webpage.url).netloc
            website = (
                base_url[4:] if base_url.startswith("www.") else base_url
            )  # If url has the prefix www., remove it
            if website != "t.me":
                url = website

        return cls(
            chat=await TelegramChat.get_or_create_by_id(chat_id),
            message_id=message_id,
            content=content,
            hash=cls.get_hash(content),
            creation_date=creation_date,
            last_retrieved=str(get_current_datetime()),
            views=telethon_message.views,
            forwards=telethon_message.forwards,
            url=url,
        )

    @classmethod
    def get_hash(cls, message_content):
        hash_size = load_json("config.json")["options"]["hash_size"]
        return str(
            hashlib.sha256(message_content.encode("utf-8")).hexdigest()[:hash_size]
        )

    def duplicate_exists(self):
        duplicate_query = TelegramMessage.select().where(
            TelegramMessage.hash == self.hash
        )
        if duplicate_query.count() != 0:
            # Duplicate found in the database (message with the same hash)
            duplicate = list(duplicate_query)[0]
            # It can happen that a message that has already been stored is found again
            # but has been modified in the meantime. In this case hash, chat_id and
            # message_id are the same but the content has changed.
            if duplicate.content == self.content or (
                duplicate.chat.id == self.chat.id
                and duplicate.message_id == self.message_id
            ):
                # Update the duplicate
                duplicate.content = self.content
                duplicate.url = self.url
                duplicate.views = self.views
                duplicate.forwards = self.forwards
                duplicate.last_retrieved = self.last_retrieved
                duplicate.save()
                return True
            else:
                raise Exception(
                    "Hash collision! The message with id",
                    duplicate.message_id,
                    "in chat",
                    duplicate.chat.id,
                    "has the same hash as the message",
                    self.message_id,
                    "in chat",
                    self.chat.id,
                    "which you want to store.",
                )
        # No duplicate found
        return False


class Topic(BaseTable):
    name = TextField(unique=True)
    short_name = TextField(unique=True)
    added = DateTimeField(default=get_current_datetime)


class Misconception(BaseTable):
    topic = ForeignKeyField(Topic)
    name = TextField(unique=True)
    short_name = TextField(unique=True)
    description = TextField()
    added = DateTimeField(default=get_current_datetime)


class MessageMisconception(BaseTable):
    chat = ForeignKeyField(TelegramChat)
    message = ForeignKeyField(TelegramMessage, db_column="message_hash")
    misconception = ForeignKeyField(Misconception, backref="messages", null=True)
    added = DateTimeField(default=get_current_datetime)


class MessageTopic(BaseTable):
    chat = ForeignKeyField(TelegramChat)
    message = ForeignKeyField(TelegramMessage, db_column="message_hash")
    topic = ForeignKeyField(Topic, backref="messages")
    added = DateTimeField(default=get_current_datetime)

    def __str__(self):
        return f"({self.message_hash_id}, {self.topic_id})"


class MessageToSkip(BaseTable):
    message = ForeignKeyField(TelegramMessage, db_column="message_hash")
    chat = ForeignKeyField(TelegramChat)
    added = DateTimeField(default=get_current_datetime)


class MisconceptionKeyword(BaseTable):
    misconception = ForeignKeyField(Misconception, backref="messages")
    word = TextField()
    positive = BooleanField()


def import_misconceptions_from_json():
    misconceptions_json = load_json("data/import/misconceptions.json")
    misconceptions = misconceptions_json["misconception"]
    for misconception in misconceptions:
        Misconception.create(
            id=misconception["id"],
            topic=misconception["topic_id"],
            name=misconception["name"],
            short_name=misconception["short_name"],
            description=misconception["description"],
            added=misconception["added"],
        )


if __name__ == "__main__":
    pass
    # db.connect()
    # db.create_tables(
    #     [
    #         TelegramChat,
    #         TelegramMessage,
    #         Topic,
    #         Misconception,
    #         MessageTopic,
    #         MessageMisconception,
    #         MessageToSkip,
    #         MisconceptionKeyword,
    #     ]
    # )
    # db.close()

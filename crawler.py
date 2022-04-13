import asyncio
import inquirer
import os
import pytz
from datetime import datetime
from telethon.errors.rpcerrorlist import ChannelPrivateError
from telethon.tl.types import MessageMediaPoll
from telethon.tl.patched import Message

# Own modules
from classifier import classify_message_binary, initialize_classifier
from models import (
    MessageMisconception,
    MessageToSkip,
    MessageTopic,
    Misconception,
    TelegramMessage,
    Topic,
)
from chat_lists import misinformation_channel_usernames
import telegram
from utility_functions import (
    get_current_datetime,
    store_json,
    load_json,
    get_or_create_json_list,
)


MESSAGES_MARKED_FOR_LATER_FILE_PATH = "data/messages_marked_for_later.json"


def get_messages_marked_for_later_index():
    if os.path.isfile(MESSAGES_MARKED_FOR_LATER_FILE_PATH):
        messages_marked_for_later_index = load_json(MESSAGES_MARKED_FOR_LATER_FILE_PATH)
    else:
        messages_marked_for_later_index = {}
    return messages_marked_for_later_index


def set_messages_marked_for_later_index(messages_marked_for_later_index):
    store_json(MESSAGES_MARKED_FOR_LATER_FILE_PATH, messages_marked_for_later_index)


class TelegramMessageHandler:
    async def handle_message(self, message):
        """
        Method that needs to be implemented by the message handler subclass. It should
        call this function through super() to set the message attribute for the whole
        class.

        Args:
            message (models.TelegramMessage or telethon.tl.patched.Message): The message
                to be handled according to the implementation of the message handler.
        """
        await self.set_message(message)
        # Do something with the message
        # ...

    async def set_message(self, message):
        """
        Set the given message as a class attribute so it can be accessed by the other
        methods. If the message is of the Telethon message type, convert it to our
        message type first.

        Args:
            message (models.TelegramMessage or telethon.tl.patched.Message): The message
                to be set for later handling.
        """
        if type(message) == Message:
            self.message = await TelegramMessage.from_telethon_message(message)
        elif type(message) == TelegramMessage:
            self.message = message
        else:
            print(
                "MessageHandler should be initialized with either a message instance "
                "from Telethon or our custom TelegramMessage class."
            )

    def message_should_be_skipped(self, skip_messages_already_stored=True):
        """
        Determines if a message should be skipped. Messages are skipped if they are
        empty or or if they have already been stored and skip_messages_already_stored is
        set to True.

        Args:
            skip_messages_already_stored (bool, optional): Defines whether to skip
                messages that are already in the database. Defaults to True.

        Returns:
            bool: True if the message should be skipped, False otherwise
        """
        return not self.message.content or (
            self.message.duplicate_exists() and skip_messages_already_stored
        )


class NoLabelingMessageHandler(TelegramMessageHandler):
    async def handle_message(self, message):
        """Handle the message by first checking if it should be skipped and if not
        storing it without labeling it.

        Args:
            message (models.TelegramMessage or telethon.tl.patched.Message): The message
                to be handled according to the implementation of the message handler.

        Returns:
            str: The status after handling the message, either "message skipped"
                or "message stored without labels"
        """
        await super().handle_message(message)
        message_should_be_skipped = self.message_should_be_skipped(
            skip_messages_already_stored=True
        )
        if message_should_be_skipped:
            return "message skipped"
        else:
            self.message.save(force_insert=True)
            return "message stored without labels"


class LabelingMessageHandler(TelegramMessageHandler):
    async def handle_message(self, message, skip_messages_already_stored=True):
        """
        Handle the message by first checking if it should be skipped and if not, then
        asking the user what to do with it. If the user chooses to do so, label the
        message. Return a status code.

        Args:
            message (models.TelegramMessage or telethon.tl.patched.Message): The message
                to be handled according to the implementation of the message handler.
            skip_messages_already_stored (bool, optional): Defines whether to skip the
                message if it already is in the database. Defaults to True.

        Returns:
            str: The status after handling the message, either "stop crawl",
                "message skipped", "message stored without labels" or
                "message stored with labels".
        """
        await super().handle_message(message)
        message_should_be_skipped = self.message_should_be_skipped(
            skip_messages_already_stored
        )
        if message_should_be_skipped:
            return "message skipped"
        else:
            return self.label_and_store_message()

    def label_and_store_message(self):
        """
        Ask the user to label the message with the topics and misconceptions it relates
        to. Then store message (if it has not been stored yet) and labels.

        Returns:
            str: The status after handling the message, either "stop crawl",
                "message stored without labels" or "message stored with labels".
        """
        (
            selected_topics,
            selected_misconceptions,
        ) = self.get_topic_and_misconception_labels_from_user()
        if len(selected_topics) == 0:
            status = self.confirm_empty_selection("topics")
        elif len(selected_misconceptions) == 0:
            status = self.confirm_empty_selection("misconceptions")
        # If the user stopped the crawl (status == "stop crawl"), return without storing
        # the message and labels, otherwise proceed
        if status == "stop crawl":
            return status
        # If the message does not exist in the database yet, store it
        if (
            TelegramMessage.select()
            .where(TelegramMessage.hash == self.message.hash)
            .count()
            == 0
        ):
            self.message.save(force_insert=True)
        else:
            # If the message already exists in the database, remove all labels so that
            # only the desired ones are re-written to the database
            MessageTopic.delete().where(
                MessageTopic.message_hash == self.message.hash
            ).execute()
            MessageMisconception.delete().where(
                MessageMisconception.message_hash == self.message.hash
            ).execute()
        # Store the associated labels of the message
        for topic in selected_topics:
            MessageTopic.create(
                chat=self.message.chat, message_hash=self.message.hash, topic=topic
            )
        for misconception in selected_misconceptions:
            MessageMisconception.create(
                chat=self.message.chat,
                message_hash=self.message.hash,
                misconception=misconception,
            )
        if len(selected_misconceptions) == 0:
            MessageMisconception.create(
                chat=self.message.chat,
                message_hash=self.message.hash,
                misconception=None,
            )
        if len(selected_misconceptions) > 0:
            return "message stored with labels"
        return "message stored without labels"

    def confirm_empty_selection(self, entity):
        """
        Make the user confirm that the empty selection of topics/misconceptions is
        correct and allow selecting different options on how to proceed.

        Args:
            entity (str): Defines what to display as the entity for which nothing was
                selected, i. e. either 'topics' or 'misconceptions'

        Returns:
            str: The status after handling the message, either "stop crawl",
                "message skipped", "message stored without labels" or
                "message stored without labels".
        """
        # If the selection of topics or misconceptions is empty, ask the user for
        # confirmation and show the different options.
        choices = [
            f"store the message without any {entity}?",
            "label the message again?",
            "skip the message without storing it?",
            "mark the message for later inspection?",
            "stop the crawl? (you can return later)",
        ]
        questions = [
            inquirer.List(
                "selection",
                message=f"You have not selected any {entity}. Do you want to",
                choices=choices,
            )
        ]
        answers = inquirer.prompt(questions)
        if answers["selection"] == f"store the message without any {entity}?":
            return "message stored without labels"
        if answers["selection"] == "label the message again?":
            return self.label_and_store_message()
        if answers["selection"] == "mark the message for later inspection?":
            messages_marked_for_later_index = get_messages_marked_for_later_index()
            # Store message as marked for later, so in the future the user will not be
            # asked again to tag it
            if str(self.message.message_id) not in messages_marked_for_later_index:
                messages_marked_for_later_index[str(self.message.chat_id)] = []
            messages_marked_for_later_index[str(self.message.chat_id)].append(
                self.message.message_id
            )
            set_messages_marked_for_later_index(messages_marked_for_later_index)
            answers["selection"] = "skip the message without storing it?"
        if answers["selection"] == "skip the message without storing it?":
            # Store skipped message, so in the future the user will not be asked again
            # to tag it
            MessageToSkip.create(message_hash=self.message.hash, chat=self.message.chat)
            return "message skipped"
        if answers["selection"] == "stop the crawl? (you can return later)":
            # Stop the crawl. It can be resumed later.
            return "stop crawl"

    def message_should_be_skipped(self, skip_messages_already_stored=True):
        """
        Determines if a message should be skipped.

        Args:
            skip_messages_already_stored (bool, optional): Defines whether to skip
                messages that are already in the database. Defaults to True.

        Returns:
            bool: True if the message should be skipped, False otherwise
        """
        if (
            super().message_should_be_skipped(
                skip_messages_already_stored=skip_messages_already_stored
            )
            is True
        ):
            return True
        # Skip messages that were skipped before
        if (
            MessageToSkip.select()
            .where(MessageToSkip.message_hash == self.message.hash)
            .count()
            != 0
        ):
            print(
                "Skipping message",
                self.message.message_id,
                "in chat",
                self.message.chat_id,
                "which has already been stored",
            )
            return True
        # Do not skip messages where any of the above does not apply
        return False

    def get_topic_and_misconception_labels_from_user(self):
        """
        Ask for user input to label the given message. The user can first select topics
        and then misconceptions related to each selected topic.

        Returns:
            list: The selected labels (topics and misconceptions)
        """
        topics = [(topic.name, topic) for topic in Topic.select()]
        selected_topics = self.prompt_selection(
            question="Which of these topics is the message related to?", options=topics
        )
        selected_misconceptions = []
        for topic in selected_topics:
            # Get all misconception related to the selected topic
            misconceptions = [
                (misconception.name, misconception)
                for misconception in Misconception.select()
                .join(Topic)
                .where(Misconception.topic == topic)
            ]
            # Let user select misconceptions related to the topic which are to be
            # assigned to the message
            selected_misconceptions.extend(
                self.prompt_selection(
                    question="Which of these misconceptions does the message support?",
                    options=misconceptions,
                )
            )
        return selected_topics, selected_misconceptions

    def prompt_selection(self, question, options):
        """
        Prompt the user to choose from the given options to characterize the message.

        Args:
            question (str): The question to answer
            options (list): List of tuples (label, object)

        Returns:
            list: List of the selected options (objects)
        """
        # os.system("clear")
        context_message = (
            f"Message {self.message.message_id} "
            f"in chat {self.message.chat.id} with hash {self.message.hash}:"
        )
        print(context_message)
        print(
            "--------------------------------------------------------------------------"
            "--------------------------------------------------------------------------"
            "------------------"
        )
        print(self.message.content)
        print(
            "--------------------------------------------------------------------------"
            "--------------------------------------------------------------------------"
            "------------------"
        )
        print(
            "\n[Press Enter without selecting any labels to go to the options menu]\n"
        )
        questions = [inquirer.Checkbox("entity", message=question, choices=options)]
        selection = inquirer.prompt(questions)
        return selection["entity"]


class BaselineCrawler:
    CRAWLS_JSON_FILE_PATH = "data/full_crawls.json"

    def __init__(self):
        pass

    @classmethod
    def store_crawl_state(self, crawl_state):
        """
        Store the current state of the crawl in the JSON file.

        Args:
            crawl_state (dict): The current crawl state including the chats crawled,
                the chats that still have to be crawled and the minimum/maximum date
                of messages to consider.
        """
        # Write crawl state to JSON
        crawls = get_or_create_json_list(self.CRAWLS_JSON_FILE_PATH)
        index = crawl_state["index"]
        if len(crawls) == index:
            crawls.append(crawl_state)
        else:
            crawls[index] = crawl_state
        store_json(self.CRAWLS_JSON_FILE_PATH, crawls)

    def retrieve_or_initialize_crawl_state(
        self,
        seed_chat_ids,
        min_date,
        max_date,
        label_message,
        crawler_specific_state_properties=[],
    ):
        """
        Check if a crawl with the same seed chats, minimum and maximum date was already
        started and can be resumed.

        Args:
            seed_chat_ids (list): Ids of chats that are/were crawled first
            min_date (datetime): The minimum date of considered messages
            max_date (datetime): The maximum date of considered messages
            label_message (bool): Determines whether each message is labeled or just
                stored without labeling
            crawler_specific_state_properties (list, optional): List of tuples, where
                each tuple consists of a property and a value in the crawl state
                qualifying a specific crawl.
        """
        no_labeling_mode = not label_message
        json_file_path = self.CRAWLS_JSON_FILE_PATH
        crawls = get_or_create_json_list(json_file_path)
        crawl_to_be_resumed_index = None
        filter_addition = ""
        for i, tuple in enumerate(crawler_specific_state_properties):
            filter_addition += f' and str(c["{tuple[0]}"]) == "{str(tuple[1])}"'
        for i, c in enumerate(crawls):
            if eval(
                'c["seed_chats"] == seed_chat_ids'
                + ' and str(c["min_date"]) == str(min_date)'
                + ' and str(c["max_date"]) == str(max_date)'
                + ' and c["no_labeling_mode"] == no_labeling_mode'
                + filter_addition
            ):
                crawl_to_be_resumed_index = i
                break
        if crawl_to_be_resumed_index is not None:
            crawl_state = crawls[crawl_to_be_resumed_index]
            crawl_state["index"] = crawl_to_be_resumed_index
            crawl_state["min_date"] = min_date  # Replace str with datetime/None
            crawl_state["max_date"] = max_date  # Replace str with datetime/None
        else:
            # Start a new crawl
            crawl_state = {
                "index": len(crawls),
                "seed_chats": seed_chat_ids,
                "min_date": min_date,
                "max_date": max_date,
                "chats_to_search": seed_chat_ids.copy(),
                "chats_searched": [],
                "started": str(get_current_datetime()),
                "no_labeling_mode": no_labeling_mode,
            }
            for i, tuple in enumerate(crawler_specific_state_properties):
                crawl_state[tuple[0]] = tuple[1]
        return crawl_state

    async def handle_chat(
        self,
        chat_id,
        crawl_state,
        crawl_should_be_stopped=None,
        message_should_be_skipped=None,
        store_crawl_state=None,
    ):
        """
        Iterating over the messages within the chat (possibly just within the time
        window restricted by min_date and/or max_date in the crawl_state) and handling
        each message. There are two modes: Either the user labels each message or the
        message is just stored without labeling it.
        Finally update and store the crawl state.

        Args:
            chat_id (int): Id of the chat to crawl
            crawl_state (dict): Description of the current crawl state
            crawl_should_be_stopped (function): Function which determines whether the
                crawl should be stopped at the current message.
            message_should_be_skipped (function): Function which determines whether the
                current message should be skipped.
            store_crawl_state (function): Function which updates and stores the current
                crawl state.

        Returns:
            bool: Return 1 if the crawl was stopped by the user (to be resumed later),
                otherwise return 0.
        """
        crawl_should_be_stopped = self.crawl_should_be_stopped
        message_should_be_skipped = self.message_should_be_skipped
        store_crawl_state = self.store_crawl_state

        min_date = crawl_state["min_date"]
        max_date = crawl_state["max_date"]

        # Iterate over messages in the chat and pass them to the message handler
        if crawl_state["no_labeling_mode"] is True:
            message_handler = NoLabelingMessageHandler()
        else:
            message_handler = LabelingMessageHandler()
        client = await telegram.get_client()
        async with client:
            try:
                async for message in client.iter_messages(
                    chat_id, offset_date=max_date
                ):
                    if crawl_should_be_stopped(
                        crawl_state, message, min_date, max_date
                    ):
                        break
                    if message_should_be_skipped(
                        crawl_state, message, min_date, max_date
                    ):
                        continue
                    status = await message_handler.handle_message(message)
                    await client.connect()  # Make sure the client stays connected (it
                    # may be disconnected after calling handle_message)
                    if crawl_state["no_labeling_mode"] is False:
                        if status == "stop crawl":
                            # User chose to stop the crawl
                            self.store_crawl_state(crawl_state)
                            return 1
                        elif (
                            status == "message skipped"
                            or "message stored without labels"
                        ):
                            pass
                        elif status == "message stored with labels":
                            # Message was stored and is relevant (at least one label was
                            # set)
                            if (
                                message_handler.message.chat.id != chat_id
                                and message_handler.message.chat.id
                                not in crawl_state["chats_searched"]
                            ):
                                # If the message was from a chat that has not been
                                # searched in the current crawl and is not yet part of
                                # the queue, add it
                                crawl_state["chats_to_search"].insert(
                                    0, message_handler.message.chat.id
                                )
                        else:
                            print("handle_message returned an invalid status:", status)
                    store_crawl_state(crawl_state)
            except ChannelPrivateError:
                print(f"Channel {chat_id} could not be crawled because it is private.")
                if "chats_not_searchable" not in crawl_state:
                    crawl_state["chats_not_searchable"] = []
                crawl_state["chats_not_searchable"].append(chat_id)
                crawl_state["chats_to_search"].pop(0)
                self.store_crawl_state(crawl_state)
                return 0

        # If a maximum date is set, the fixed time window of relevant messages in
        # the current chat has been crawled at this point. Therefore the chat id can
        # be added to chats_searched.
        # If no maximum date is set, the crawl is never finished and the chat id is
        # just moved to the end of the queue, to be searched again in the future
        # when there might be new messages.
        if max_date is not None:
            crawl_state["chats_searched"].append(chat_id)
        else:
            crawl_state["chats_to_search"].append(chat_id)
        crawl_state["chats_to_search"].pop(0)
        self.store_crawl_state(crawl_state)
        return 0

    def crawl_should_be_stopped(self, crawl_state, message, min_date, max_date):
        """
        Determine whether the crawl should be stopped at the current message. The crawl
        is stopped if the current message has been posted before the minimum date.

        Args:
            crawl_state (dict): Current crawl state
            message (telethon.tl.patched.Message): The current message
            min_date (datetime.datetime): Minimum date to consider
            max_date (datetime.datetime): Maximum date to consider

        Returns:
            bool: True if the crawl should be stopped, False otherwise
        """
        if min_date is not None and message.date < min_date:
            # If the current message is older than what was set as the
            # minimum date, stop crawling the current chat
            print(
                f"Crawl of chat {message.peer_id.channel_id} stopped because the next "
                f"message was posted before "
                f'{ min_date.astimezone(pytz.timezone("Europe/Berlin")) }'
            )
            return True
        return False

    def message_should_be_skipped(self, crawl_state, message, min_date, max_date):
        """
        Determine whether the current message should be skipped. Messages are skipped if
        they are empty, were posted after the maximum date or are polls.

        Args:
            crawl_state (dict): Current crawl state
            message (telethon.tl.patched.Message): The current message
            min_date (datetime.datetime): Minimum date to consider
            max_date (datetime.datetime): Maximum date to consider

        Returns:
            bool: True if the message should be skipped, False otherwise
        """
        return (
            message.message is None
            or (max_date is not None and message.date > max_date)
            or (message.media and type(message.media) == MessageMediaPoll)
        )


class KeywordSearchCrawler(BaselineCrawler):
    CRAWLS_JSON_FILE_PATH = "data/keyword_search_crawls.json"

    def __init__(self, keywords):
        self.keywords = keywords

    def retrieve_or_initialize_crawl_state(
        self, seed_chat_ids, min_date, max_date, label_message=True
    ):
        """
        Check if a crawl with the same seed chats, minimum and maximum date and list of
        keywords was already started and can be resumed.

        Args:
            seed_chat_ids (list): Ids of chats that are/were crawled first
            min_date (datetime): The minimum date of considered messages
            max_date (datetime): The maximum date of considered messages
            label_message (bool): Determines whether each message is labeled or just
                stored without labeling
        """
        return super().retrieve_or_initialize_crawl_state(
            seed_chat_ids,
            min_date,
            max_date,
            label_message,
            crawler_specific_state_properties=[
                ("keywords", self.keywords),
            ],
        )

    def handle_chat(self, chat_id, crawl_state):
        """
        Handles the given chat by iterating over its messages (possibly just within the
        time window restricted by min_date and/or max_date in the crawl_state), finding
        all messages containing certain keywords and making the user label these.

        Args:
            chat_id (int): Id of the chat to crawl
            crawl_state (dict): Description of the current crawl state

        Returns:
            bool: Return 1 if the crawl was stopped by the user (to be resumed later),
                otherwise return 0.
        """
        return super().handle_chat(
            chat_id,
            crawl_state,
            self.crawl_should_be_stopped,
            self.message_should_be_skipped,
        )

    def message_should_be_skipped(self, crawl_state, message, min_date, max_date):
        """
        Determine whether the current message should be skipped. Messages are skipped if
        they were posted after the maximum date or if they contain none of the keywords.

        Args:
            crawl_state (dict): Current crawl state
            message (telethon.tl.patched.Message): The current message
            min_date (datetime.datetime): Minimum date to consider
            max_date (datetime.datetime): Maximum date to consider

        Returns:
            bool: True if the message should be skipped, False otherwise
        """
        # Skip messages posted after the maximum date
        if super().message_should_be_skipped(crawl_state, message, min_date, max_date):
            return True
        # Check if any of the keywords is contained in the message, otherwise skip
        for keyword in self.keywords:
            if keyword in message.message:
                return False
        return True


class BinaryClassifierCrawler(BaselineCrawler):
    CRAWLS_JSON_FILE_PATH = "data/binary_classifier_crawls.json"

    def __init__(self, base_model, model_directory):
        self.base_model = base_model
        self.model_directory = model_directory
        self.classifier = initialize_classifier(self.model_directory, 2)

    def retrieve_or_initialize_crawl_state(self, seed_chat_ids, min_date, max_date):
        """
        Check if a crawl with the same seed chats, minimum and maximum date and
        classification model was already started and can be resumed.

        Args:
            seed_chat_ids (list): Ids of chats that are/were crawled first
            min_date (datetime): The minimum date of considered messages
            max_date (datetime): The maximum date of considered messages
        """
        crawl_state = super().retrieve_or_initialize_crawl_state(
            seed_chat_ids,
            min_date,
            max_date,
            crawler_specific_state_properties=[
                ("base_model", self.base_model),
                ("model_directory", self.model_directory),
            ],
        )
        if "current_chat_classified_messages_ids" not in crawl_state:
            crawl_state["current_chat_classified_messages_ids"] = []
        return crawl_state

    def handle_chat(self, chat_id, crawl_state):
        """
        Handles the given chat by iterating over its messages (possibly just within the
        time window restricted by min_date and/or max_date in the crawl_state), passing
        messages to the binary classifier which determine if the message is relevant and
        passing each message deemed relevant to the user to label it.

        Args:
            chat_id (int): Id of the chat to crawl
            crawl_state (dict): Description of the current crawl state

        Returns:
            bool: Return 1 if the crawl was stopped by the user (to be resumed later),
                otherwise return 0.
        """
        chat_handler_result = super().handle_chat(
            chat_id,
            crawl_state,
            self.crawl_should_be_stopped,
            self.message_should_be_skipped,
            self.store_crawl_state,
        )
        if "classified_chat_until" not in crawl_state:
            crawl_state["classified_chat_until"] = {}
        if len(crawl_state["current_chat_classified_messages_ids"]) > 0:
            # Store the most recent classified message, so when checking the chat
            # for new messages when resuming the crawl in the future, this and all
            # prior messages are not classified again.
            crawl_state["classified_chat_until"][str(chat_id)] = crawl_state[
                "current_chat_classified_messages_ids"
            ][0]
            # Reset classified ids because current chat is done
            crawl_state["current_chat_classified_messages_ids"] = []
        return chat_handler_result

    def crawl_should_be_stopped(self, crawl_state, message, min_date, max_date):
        """
        Determine whether the crawl of the chat should be stopped at the current
        message. The crawl is stopped if the current message was posted before the
        minimum date or if the crawl was resumed and the point is reached from which on
        all messages in the chat have already been classified.

        Args:
            crawl_state (dict): Current crawl state
            message (telethon.tl.patched.Message): The current message
            min_date (datetime.datetime): Minimum date to consider
            max_date (datetime.datetime): Maximum date to consider

        Returns:
            bool: True if the crawl should be stopped, False otherwise
        """
        # Stop the crawl, if the current message was posted before the minimum date
        if (
            super().crawl_should_be_stopped(crawl_state, message, min_date, max_date)
            is True
        ):
            return True
        if (
            max_date is None
            and "classified_chat_until" in crawl_state
            and str(message.peer_id.channel_id) in crawl_state["classified_chat_until"]
            and message.id
            <= crawl_state["classified_chat_until"][str(message.peer_id.channel_id)]
        ):
            # If there is no max date and the current chat has been crawled
            # fully before (but now again because there may be new messages),
            # and the crawler has reached the point from which on messages have
            # already been classified, then it can stop.
            return True
        return False

    def message_should_be_skipped(self, crawl_state, message, min_date, max_date):
        """
        Determine whether the current message should be skipped. Messages are skipped if
        they were posted after the maximum date, if they have already been classified in
        the current crawl or if they are classified as irrelevant.

        Args:
            crawl_state (dict): Current crawl state
            message (telethon.tl.patched.Message): The current message
            min_date (datetime.datetime): Minimum date to consider
            max_date (datetime.datetime): Maximum date to consider

        Returns:
            bool: True if the message should be skipped, False otherwise
        """
        # Skip messages posted after the maximum date
        if super().message_should_be_skipped(crawl_state, message, min_date, max_date):
            return True
        # Skip messages that have no content or have already been classified before
        # (potentially as irrelevant)
        if (
            message.message is None
            or message.id in crawl_state["current_chat_classified_messages_ids"]
        ):
            return True

        classification_result = classify_message_binary(
            classifier=self.classifier,
            base_model=self.base_model,
            input_message=message.message,
            threshold=0.5,
        )
        # print(
        #     f"Classification result of message {message.id} in chat "
        #     f"{message.peer_id.channel_id}: {classification_result}"
        # )
        if classification_result[0] == "irrelevant":
            # Store message id to prevent it from being classified again
            crawl_state["current_chat_classified_messages_ids"].append(message.id)
            return True
        return False

    def store_crawl_state(self, status, message, crawl_state):
        """
        Store the current state of the crawl in the JSON file. Add the id of the most
        recently classified message, so it does not have to be classified again in the
        future, if the crawl is interrupted and later resumed.

        Args:
            crawl_state (dict): The current crawl state with the addition of the most
                recently classified message (id).
        """
        # Add message id to the crawl state
        if message.id is not None:
            crawl_state["current_chat_classified_messages_ids"].append(message.id)
        # Store crawl state in JSON file
        super().store_crawl_state(crawl_state)


class CrawlController:
    """
    This class controls the basic process of a crawl. It calls more specific crawler
    classes which deal with pre-filtering, managing the crawl state and calling the
    message handler.
    """

    def __init__(self, crawler):
        self.crawler = crawler

    async def crawl(
        self,
        seed_chats,
        min_date=None,
        max_date=None,
        label_messages=True,
    ):
        """
        Collect training data by letting the user label messages that are crawled
        starting in the given seed chats and then continuing in chats from which
        relevant messages have been forwarded.

        Args:
            seed_chats (list): List of ids or usernames of the chats to search first
            min_date (tuple, optional): A tuple consisting of year, month, day, ...
                of the minimum datetime to consider. When reaching a message that is
                older than this, the crawl of the current chat is stopped.
                Defaults to None.
            max_date (tuple, optional): A tuple consisting of year, month, day, ...
                of the maximum datetime to consider. All messages that are newer
                than this are skipped. Defaults to None.
        """
        # If seed_chats contains usernames, replace them with ids
        seed_chat_ids = []
        for chat in seed_chats:
            if type(chat) == str:
                chat_info = await telegram.get_chat_info(chat)
                chat_id = chat_info["chats"][0]["id"]
                seed_chat_ids.append(chat_id)
            else:
                seed_chat_ids.append(chat)

        # Convert min and max_date to datetime
        if min_date is not None:
            min_date = datetime(*min_date).astimezone(pytz.UTC)
        if max_date is not None:
            max_date = (
                datetime(*max_date)
                .replace(hour=23, minute=59, second=59, microsecond=999999)
                .astimezone(pytz.UTC)
            )

        crawl_state = self.crawler.retrieve_or_initialize_crawl_state(
            seed_chat_ids, min_date, max_date, label_messages
        )
        await self.start_or_resume_crawl(
            crawl_state
        )  # modifies crawl_state and stores it in JSON file

    async def start_or_resume_crawl(self, crawl_state):
        """
        Start or resume a crawl according to the given crawl state and the specific
        crawler class this class was initialized with. Each relevant message is shown to
        the user to determine what to do with it. If a relevant message (labeled with at
        least one misconception) was forwarded from another chat that has not been seen
        before during the crawl, this chat is added to the queue of chats to search.

        Args:
            crawl_state (dict): Dictionary representing the current state of the
                crawl. Contains the properties such as chats_to_search (list),
                chats_searched (list) and additional crawler-specific properties.
        """
        # Keep a list of chats that were crawled fully in the current run to prevent an
        # infinite loop when there is no max_date and all chats have be crawled until
        # the most recent messages
        stop_check = []
        no_new_messages = 0

        # Start/resume crawl
        while len(crawl_state["chats_to_search"]) > 0:
            current_chat_id = crawl_state["chats_to_search"][0]
            if current_chat_id in stop_check:
                no_new_messages = 1
                break

            crawl_stopped = await self.crawler.handle_chat(current_chat_id, crawl_state)
            if crawl_stopped == 1:
                break
            stop_check.append(current_chat_id)

        if len(crawl_state["chats_to_search"]) == 0 or no_new_messages == 1:
            print("Finished: No more chats to search")
            crawl_state["finished"] = str(get_current_datetime())
        # Store crawl state
        self.crawler.store_crawl_state(crawl_state)


def relabel_message(message_hash):
    """
    Ask the user to label a stored message again and update the labels. Messages that
    were skipped cannot be relabeled.

    Args:
        message_hash (str): Hash of the message to re-label
    """
    message = TelegramMessage.get_by_id(message_hash)
    message_handler = LabelingMessageHandler(message)
    message_handler.label_and_store_message()


# if __name__ == "__main__":
async def main():
    crawler = CrawlController(BaselineCrawler())
    await crawler.crawl(
        misinformation_channel_usernames,
        min_date=(2021, 7, 1),
        max_date=(2021, 7, 31),
        label_messages=False,
    )


asyncio.run(main())

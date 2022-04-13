import json
import os

from models import MessageMisconception, TelegramMessage, Misconception, Topic
from keywords import positive_keywords, negative_keywords
from utility_functions import store_json


def create_messages_jsonl_by_id_list(
    ids, max_message_length, max_train_messages, filter_using_keywords=False
):
    """
    Export messages by creating a jsonl file with all messages in the given list of ids.
    The jsonl file format is adopted from the CoVaxLies dataset.

    Args:
        ids (list): Ids of the messages to export.
        max_message_length (int): Maximum length of the messages to export.
        filter_using_keywords (bool): Whether to filter out messages based on keywords.
            Defaults to False.
    """
    if not os.path.exists("data/export/"):
        os.makedirs("data/export/")
    with open("data/export/messages.jsonl", "w") as output_file:
        train_messages = 0
        for id in ids:
            if train_messages == max_train_messages:
                break
        for id in ids:
            message = TelegramMessage.get(TelegramMessage.hash == id)
            include = True
            if len(message.content) > max_message_length:
                include = False
            if filter_using_keywords:
                # Only include messages containing one of the positive keywords
                include = False
                for keyword in positive_keywords:
                    if keyword.lower() in message.content.lower():
                        include = True
                        break
                # Only include messages containing none of the negative keywords
                for keyword in negative_keywords:
                    if keyword.lower() in message.content.lower():
                        include = False
                        break
            if include is True:
                message_misconceptions_object = {}
                for message_misconception in MessageMisconception.select().where(
                    MessageMisconception.message == message
                ):
                    message_misconceptions_object[
                        message_misconception.misconception.id
                    ] = "agree"
                message_object = {
                    "id": message.hash,
                    "misinfo": message_misconceptions_object,
                    "full_text": message.content,
                }
                output_file.write(json.dumps(message_object, ensure_ascii=False) + "\n")
                train_messages += 1


def create_messages_jsonl(max_message_length, filter_using_keywords=False):
    """
    Export messages by creating jsonl file with all messages in the database (or the
    ones that remain after filtering).
    The jsonl file format is adopted from the CoVaxLies dataset.

    Args:
        max_message_length (int): Maximum length of the messages to export.
        filter_using_keywords (bool): Whether to filter out messages based on keywords.
            Defaults to False.
    """
    if not os.path.exists("data/export/"):
        os.makedirs("data/export/")
    with open("data/export/train.jsonl", "w") as output_file:
        for message in TelegramMessage.select():
            include = True
            if len(message.content) > max_message_length:
                include = False
            if filter_using_keywords:
                # Only include messages containing one of the positive keywords
                include = False
                for keyword in positive_keywords:
                    if keyword.lower() in message.content.lower():
                        include = True
                        break
                # Only include messages containing none of the negative keywords
                for keyword in negative_keywords:
                    if keyword.lower() in message.content.lower():
                        include = False
                        break
            if include is True:
                message_misconceptions_object = {}
                for message_misconception in MessageMisconception.select().where(
                    MessageMisconception.message == message
                ):
                    message_misconceptions_object[
                        message_misconception.misconception.id
                    ] = "agree"
                message_object = {
                    "id": message.hash,
                    "misinfo": message_misconceptions_object,
                    "full_text": message.content,
                }
                output_file.write(json.dumps(message_object, ensure_ascii=False) + "\n")


def create_misinfo_json():
    """
    Creates a json file with all misconceptions in the database.
    The json file format is adopted from the CoVaxLies dataset.
    """
    misinfo_json = {}
    for misconception in Misconception.select():
        misinfo_json[misconception.id] = {
            "title": misconception.short_name,
            "text": misconception.name,
            "alternate_text": misconception.description,
        }
    store_json("data/export/misinfo.json", misinfo_json)


def create_topics_json():
    """
    Creates a JSON file containing topic objects which can be imported into the
    labeling Web interface.
    """
    if not os.path.exists("data/export/interface/"):
        os.makedirs("data/export/interface/")
    topic_list = []
    for topic in Topic.select():
        topic_list.append({"name": topic.name})
    out_file = open("data/export/interface/topics.json", "w")
    json.dump(topic_list, out_file, indent=4, sort_keys=True, ensure_ascii=False)
    out_file.close()


def create_misconceptions_json():
    """
    Creates a JSON file containing misconception objects which can be imported into the
    labeling Web interface.
    """
    if not os.path.exists("data/export/interface/"):
        os.makedirs("data/export/interface/")
    misconception_list = []
    for misconception in Misconception.select():
        misconception_list.append(
            {
                "name": misconception.name,
                "short_name": misconception.short_name,
                "description": misconception.description,
                "topic": misconception.topic_id,
            }
        )
    out_file = open("data/export/interface/misconceptions.json", "w")
    json.dump(
        misconception_list, out_file, indent=4, sort_keys=True, ensure_ascii=False
    )
    out_file.close()


def create_messages_json(message_qualifying_query=None):
    """
    Creates a JSON file containing message objects which can be imported into the
    labeling Web interface.

    Args:
        message_qualifying_query (_type_, optional): Specifies the ids of misconceptions
            which are not excluded in the export. Messages which only have this
            misconceptions as a label are also not included. Defaults to None.

    Example usage:
        create_messages_json(
            message_qualifying_query="SELECT * FROM telegrammessage WHERE creation_date > '2021-12-01'",
        )
    """
    if not os.path.exists("data/export/interface/"):
        os.makedirs("data/export/interface/")
    messages_list = []
    if message_qualifying_query is not None:
        messages = TelegramMessage.raw(message_qualifying_query)
    else:
        messages = TelegramMessage.select()
    for message in messages:
        messages_list.append(
            {
                "content": message.content,
                "id": message.hash,
            }
        )
    out_file = open("data/export/interface/messages.json", "w")
    json.dump(messages_list, out_file, indent=4, sort_keys=True, ensure_ascii=False)
    out_file.close()


def create_messages_json_from_jsonl(jsonl_path):
    """
    Creates a JSON file containing message objects which can be imported into the
    labeling Web interface based on a jsonl file that uses the data format adopted from
    the CoVaxLies dataset.

    Args:
        jsonl_path (str): Path to the jsonl file.
    """
    if not os.path.exists("data/export/interface/"):
        os.makedirs("data/export/interface/")
    with open(jsonl_path, "r") as json_file:
        json_list = list(json_file)
    messages_list = []
    for json_str in json_list:
        line_object = json.loads(json_str)
        messages_list.append(
            {"content": line_object["full_text"], "id": line_object["id"]}
        )
    out_file = open("data/export/interface/messages.json", "w")
    json.dump(messages_list, out_file, indent=4, sort_keys=True, ensure_ascii=False)
    out_file.close()


if __name__ == "__main__":
    pass
    # Code used to create the final labeling data set
    # See standalone scripts repository for the function
    # create_cumulative_message_retrieval_ranking that is used to create the list
    # create_messages_jsonl_by_id_list(
    #     list(
    #         map(
    #             lambda x: x[0],
    #             load_json(
    #                 "train-cumulative-bm25-ranking.json"
    #             ),
    #         )
    #     ),
    #     10000,
    #     1000,
    # )

    # Code for creating files for Web interface import
    # create_topics_json()
    # create_misconceptions_json()
    # create_messages_json(
    #     "SELECT * FROM telegrammessage WHERE creation_date > '2022-02-28'"
    # )

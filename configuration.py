import inquirer

# Own modules
from models import db, MisconceptionKeyword, BaseTable, Misconception, Topic
from utility_functions import load_json


def create_tables():
    # Create all tables for all models
    db.create_tables(BaseTable.__subclasses__())


def add_topic(name=None, short_name=None):
    if name is None:
        print("Enter the name of the topic you want to add")
        name = input()
    if short_name is None:
        print("Enter a short name to identify the topic by (snake_case)")
        short_name = input()
    topic = Topic(name=name, short_name=short_name)
    topic.save()
    return topic


def add_misconception(name=None, short_name=None, description=None, topic=None):
    if topic is None:
        # Select the topic the new misconception belongs to
        topic_options = [(topic.name, topic.id) for topic in Topic.select()]
        topic_options.append(("None of the above (Add new topic)", 0))
        questions = [
            inquirer.List(
                "topic",
                message="What topic does the misconception belong to?",
                choices=topic_options,
            )
        ]
        selection = inquirer.prompt(questions)
        topic_id = selection["topic"]
        if topic_id == 0:
            add_topic()
            # Start from the beginning
            add_misconception()
            return
    if name is None:
        print("Enter the name of the misconception you want to add")
        name = input()
    if short_name is None:
        print("Enter a short name to identify the misconception by (snake_case)")
        short_name = input()
    if description is None:
        print("Enter a description for the misconception")
        description = input()
    misconception = Misconception(
        topic=topic_id, name=name, short_name=short_name, description=description
    )
    misconception.save()
    return misconception


def add_misconception_keyword(misconception_short_name=None, word=None, positive=None):
    if misconception_short_name is None:
        # Select the misconception to which to add the keyword
        misconception_options = [
            misconception.short_name for misconception in Misconception.select()
        ]
        questions = [
            inquirer.List(
                "misconception",
                message="What misconception should the keyword be added to?",
                choices=misconception_options,
            )
        ]
        selection = inquirer.prompt(questions)
        misconception_short_name = selection["misconception"]
    misconception_id = Misconception.get(
        Misconception.short_name == misconception_short_name
    )
    if word is None:
        print(
            "Enter the word you want to add to the positive or negative keyword list "
            "of the misconception"
        )
        word = input()
    if positive is None:
        questions = [
            inquirer.List(
                "positive_or_negative",
                message="Does the word belong to the positive or negative keyword "
                "list?",
                choices=["positive", "negative"],
            )
        ]
        selection = inquirer.prompt(questions)
        positive = True if selection["positive_or_negative"] == "positive" else False
    keyword = MisconceptionKeyword(
        misconception=misconception_id, word=word, positive=positive
    )
    keyword.save()
    return keyword


def add_labels_from_json(file_path):
    """
    Load topics and misconceptions from a JSON file into the database.
    The keys are the topic names and the values are lists of misconception names.

    Args:
        file_path (str): Path of the JSON file
    """
    labels = load_json(file_path)
    for topic_name in labels.keys():
        add_topic(topic_name)
        topic = Topic.select().where(Topic.name == topic_name).get()
        for misconception_name in labels[topic_name]:
            add_misconception(name=misconception_name, topic=topic)


if __name__ == "__main__":
    # Calls to initialize the topics and misconceptions tables. Uncomment and run once.
    # create_tables()
    # add_labels_from_json('labels.json')

    # Calls to add topics, misconceptions or keywords
    # add_topic()
    add_misconception()
    # add_misconception_keyword()

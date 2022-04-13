# Code that is no longer used

import nltk
import os
import pandas as pd

from models import (
    MessageMisconception,
    TelegramMessage,
    MisconceptionKeyword,
    Misconception,
    TelegramChat,
)


def get_multilabel_ground_truth_data_frame(
    misconceptions_to_consider, drop_irrelevant_rows, remove_stop_words
):
    """
    Builds and stores (as .csv) a one hot encoding pandas dataframe of all messages and
    their corresponding misconception labels. Only considers the given misconceptions.

    Args:
        misconceptions_to_consider (list): List of misconceptions that should be
            included
        drop_irrelevant_rows (bool): Whether to drop messages that have no misconception
            labels assigned
        remove_stop_words (bool): Whether to remove stop words from the message content
            before storing it in the dataframe

    Returns:
        pandas.core.frame.DataFrame: Pandas dataframe containing the messages and their
            ground truth misconception labels
    """
    # Inizialize dataframe
    misconception_short_names = [
        misconception.short_name for misconception in misconceptions_to_consider
    ]
    df_labeled_messages = pd.DataFrame(columns=["content"] + misconception_short_names)
    # Add messages and labels to dataframe
    misconception_labels = MessageMisconception.select().where(
        MessageMisconception.misconception.in_(misconceptions_to_consider)
    )
    zeros = [0 for i in range(len(misconception_short_names))]
    for message in TelegramMessage.select():
        if remove_stop_words:
            # Remove stop words
            german_stop_words = nltk.corpus.stopwords.words("german")
            input_message_split = message.content.split()
            processed_message = " ".join(
                [w for w in input_message_split if w not in german_stop_words]
            )
            df_labeled_messages.loc[message.hash] = [processed_message] + list(zeros)
        else:
            df_labeled_messages.loc[message.hash] = [message.content] + list(zeros)
    for misconception_label in misconception_labels:
        df_labeled_messages.loc[
            misconception_label.message_hash,
            misconception_label.misconception.short_name,
        ] = 1
    if drop_irrelevant_rows:
        # Iterate over rows and drop the ones with zero for all labels
        for index, value in df_labeled_messages.iterrows():
            labels = list(value[2:])
            if labels == [0 for i in range(len(labels))]:
                df_labeled_messages.drop(index, axis=0, inplace=True)
    df_labeled_messages.index.name = "message_hash"
    return df_labeled_messages


def print_label_counts():
    df_messages = get_multilabel_ground_truth_data_frame(
        Misconception.select(), False, False
    )
    label_columns = list(df_messages.columns[1:])
    print("Label counts:")
    print(df_messages[label_columns].sum(), "\n")


def create_multilabel_csv_data(
    minimum_messages_for_label, drop_irrelevant_rows=False, remove_stop_words=True
):
    """
    Build and store (as .csv) two pandas dataframes - one containing the messages and
    their true misconception labels, the other containing the messages and a baseline
    prediction of misconception labels based on the positive and negative keywords for
    each misconception. Only consider misconceptions with a certain number of training
    examples.

    Args:
        minimum_messages_for_label (int, optional): The minimum number of training
            examples for a misconception to be considered. Defaults to 0.
        drop_irrelevant_rows (bool, optional): Whether to messages that have no
            misconception labels assigned. Defaults to False.
        remove_stop_words (bool, optional): Whether to remove stop words from the
            message content before storing it in the dataframe. Defaults to False.
    """
    csv_file_suffix = ""
    # Determine which misconceptions to consider
    misconceptions_to_consider = []
    for misconception in Misconception.select():
        number_of_messages = len(
            MessageMisconception.select().where(
                MessageMisconception.misconception == misconception.id
            )
        )
        if number_of_messages >= minimum_messages_for_label:
            misconceptions_to_consider.append(misconception)

    df_ground_truth = get_multilabel_ground_truth_data_frame(
        misconceptions_to_consider, drop_irrelevant_rows, remove_stop_words
    )
    if remove_stop_words:
        csv_file_suffix = "_condensed"
    if not os.path.exists("data/training_data_sets/"):
        os.makedirs("data/training_data_sets/")
    df_ground_truth.to_csv(
        f"data/training_data_sets/labeled_messages_ground_truth{csv_file_suffix}.csv"
    )
    # Build baseline prediction dataframe
    df_baseline_prediction = df_ground_truth
    positive_keywords = MisconceptionKeyword.select().where(
        MisconceptionKeyword.positive is True
    )
    negative_keywords = MisconceptionKeyword.select().where(
        MisconceptionKeyword.positive is False
    )
    zeros = [0 for i in range(len(df_baseline_prediction.columns) - 1)]
    for index, value in df_baseline_prediction.iterrows():
        df_baseline_prediction.loc[index] = [value[0]] + list(zeros)
        for keyword in positive_keywords:
            if keyword.word in value[0]:
                misconception_short_name = Misconception.get_by_id(
                    keyword.misconception
                ).short_name
                df_baseline_prediction.loc[index, misconception_short_name] = 1
        for keyword in negative_keywords:
            if keyword.word in value[0]:
                misconception_short_name = Misconception.get_by_id(
                    keyword.misconception
                ).short_name
                df_baseline_prediction.loc[index, misconception_short_name] = 0
    df_baseline_prediction.index.name = "message_hash"
    df_baseline_prediction.to_csv(
        f"data/training_data_sets/labeled_messages_keywords_baseline_prediction{csv_file_suffix}.csv"
    )


def create_binary_csv_data(remove_stop_words=False):
    csv_file_suffix = ""
    # Inizialize dataframe
    df_binary = pd.DataFrame(columns=["content", "relevant"])
    # Add messages with a label of either relevant or irrelevant to the dataframe
    for message in TelegramMessage.select():
        # Check if there is a misconception label for this message
        relevant = 0
        if (
            MessageMisconception.select()
            .where(MessageMisconception.message == message)
            .count()
            != 0
        ):
            relevant = 1
        if remove_stop_words:
            csv_file_suffix = "_condensed"
            # Remove stop words
            german_stop_words = nltk.corpus.stopwords.words("german")
            input_message_split = message.content.split()
            processed_message = " ".join(
                [w for w in input_message_split if w not in german_stop_words]
            )
            df_binary.loc[message.hash] = [processed_message, relevant]
        else:
            df_binary.loc[message.hash] = [message.content, relevant]
    df_binary.index.name = "message_hash"
    # Split into two data frames - train and test set
    df_train = df_binary.sample(frac=0.9)
    df_test = df_binary.drop(df_train.index)
    # Store data frames as csv files
    if not os.path.exists("data/training_data_sets/"):
        os.makedirs("data/training_data_sets/")
    df_train.to_csv(
        f"data/training_data_sets/labeled_messages_binary{csv_file_suffix}_train.csv"
    )
    df_test.to_csv(
        f"data/training_data_sets/labeled_messages_binary{csv_file_suffix}_test.csv"
    )


def get_usernames_from_chat_id_list(chat_ids):
    """
    Get the usernames of the chats that are in the given list of chat ids.

    Args:
        chat_ids (list): User ids of the chats.

    Returns:
        list: Usernames of the chats.
    """
    usernames = []
    for chat_id in chat_ids:
        usernames.append(TelegramChat.get(TelegramChat.id == chat_id).username)
    return usernames


if __name__ == "__main__":
    # Example usage
    create_multilabel_csv_data(
        minimum_messages_for_label=30,
        drop_irrelevant_rows=True,
        remove_stop_words=True,
    )
    create_binary_csv_data(remove_stop_words=True)

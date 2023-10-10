# -*- coding: utf-8 -*-


def test_new_chat(awd):
    chat = awd.get_chat()
    chat_id = chat.new_chat(user_id="user123")
    assert chat_id is not None


def test_append_messages(awd):
    chat = awd.get_chat()
    chat_id = chat.new_chat(user_id="user123")
    messages = [
        {"role": "user", "said": "Hello!"},
        {"role": "assistant", "said": "Hi there!"}
    ]
    chat.append_messages(chat_id, messages)

    fetched_messages = chat.get_messages(chat_id)
    assert len(fetched_messages) == 2
    assert fetched_messages[0]["role"] == "user"
    assert fetched_messages[0]["said"] == "Hello!"
    assert fetched_messages[1]["role"] == "assistant"
    assert fetched_messages[1]["said"] == "Hi there!"


def test_get_messages_empty(awd):
    chat = awd.get_chat()
    chat_id = chat.new_chat(user_id="user123")
    fetched_messages = chat.get_messages(chat_id)
    assert len(fetched_messages) == 0

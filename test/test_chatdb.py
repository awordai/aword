# -*- coding: utf-8 -*-


def test_new_chat(awd):
    chatdb = awd.get_chatdb()
    chat_id = chatdb.new_chat(tenant_id="test_tenant", user_id="user123")
    assert chat_id is not None


def test_append_messages(awd):
    chatdb = awd.get_chatdb()
    chat_id = chatdb.new_chat(tenant_id="test_tenant", user_id="user123")
    messages = [
        {"role": "user", "content": "Hello!"},
        {"role": "assistant", "content": "Hi there!"}
    ]
    chatdb.append_messages(chat_id, messages)

    fetched_messages = chatdb.get_messages(chat_id)
    assert len(fetched_messages) == 2
    assert fetched_messages[0]["role"] == "user"
    assert fetched_messages[0]["content"] == "Hello!"
    assert fetched_messages[1]["role"] == "assistant"
    assert fetched_messages[1]["content"] == "Hi there!"


def test_get_messages_empty(awd):
    chatdb = awd.get_chatdb()
    chat_id = chatdb.new_chat(tenant_id="test_tenant", user_id="user123")
    fetched_messages = chatdb.get_messages(chat_id)
    assert len(fetched_messages) == 0

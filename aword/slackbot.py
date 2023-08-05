# -*- coding: utf-8 -*-

import os
import re
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# from aword import chat


SlackApp = App(token=os.environ["SLACK_BOT_TOKEN"])


def process_question(body, say):
    say('hola que tal')
    # channel = body["channel"]
    # event_ts = body["ts"]
    # question = body["text"].strip().replace("<@(.*?)>", "", re.IGNORECASE)
    #
    # SlackApp.client.reactions_add(channel=channel, name="brain", timestamp=event_ts)
    #
    # try:
    #     answer = chat.process_question(question)
    #     SlackApp.client.reactions_remove(channel=channel, name="brain", timestamp=event_ts)
    #     say(answer)
    #
    # except Exception as err:
    #     print(f"Error: {err}")
    #     say(":x: An error occurred while fetching the answer. Please try again later.")


@SlackApp.event("message")
def handle_message(body, say):
    say('nose')
    print('11')
    if body["event"]["channel_type"] != "im":
        return
    process_question(body["event"], say)


@SlackApp.event("app_mention")
def handle_app_mention(body, say):
    print('22')
    say('ayayay')
    process_question(body, say)


if __name__ == "__main__":
    handler = SocketModeHandler(SlackApp, os.environ["SLACK_APP_TOKEN"])
    handler.start()
    print("⚡️ Aword is running!")

# Aword

Aword is a GPT-powered information retrieval system that embeds pieces of information coming from several sources, stores the embeddings in a vector database, and enables querying based on the embeddings.

## Running and configuration

Aword tries to load environment variables from `.env`, or `.env.test` when run by `pytest`.

### Main configuration options

Aword uses a `config.ini` file for most configuration options. You can provide this file in one of three ways:

1. Place it in the root of the repository.
2. Set the `AWORD_CONFIG` environment variable.
3. Place it in `~/.aword/config.ini`.

Here is an example of a `config.ini` file:

```ini
[state]
last_seen_db = res/test/last-seen.json

[qdrant]
qdrant_collection = aword
qdrant_local_db = res/test/local.qdrant
qdrant_suid_field = source_unit_id
qdrant_text_field = text

# if QDRANT_URL is defined it will use that
# qdrant_url =

[openai]
oai_embedding_dimensions = 1536
oai_embedding_model = text-embedding-ada-002
oai_embedding_ctx_length = 8191
oai_embedding_encoding = cl100k_base
oai_embedding_chunk_size = 400
oai_max_texts_to_embed_batch_size = 100
oai_n_chunks_for_context = 8
oai_model = gpt-3.5-turbo
oai_system_prompt = res/system-prompt.txt
```

### Configuration of sources

Aword can import data from several sources. They are configured with a json file. You can provide it in one of three different ways:

1. Place a `sources.json` file in the root of the repository.
2. Set the `AWORD_SOURCES_CONFIG` environment variable.
3. Place it in `~/.aword/sources.json`.

The sources definition looks like this:

```json
{
  "notion": {
    "databases": [
      {
        "id": "482...",
        "doc": "Shared notes",
        "fact_type": "reference"
      },
      {
        "id": "fe8...",
        "doc": "User research notes",
        "fact_type": "historical"
      }
    ],
    "pages": [
      {
        "id": "c50...",
        "doc": "Project A documentation",
        "fact_type": "reference"
      },
      {
        "id": "e487...",
        "doc": "Meeting notes",
        "fact_type": "historical"
      }
    ]
  },
  "local": [
    {
      "directory": "res/test/local",
      "extensions": ["md", "org", "txt"],
      "author": "unknown",
      "fact_type": "reference"
    },
    {
      "directory": "res/test/local/short",
      "author": "John Doe",
      "fact_type": "historical"
    }
  ]
}

```

### Access to the OpenAI API

The environment variable `OPENAI_API_KEY` is always required.

### Embedding database API

Embeddings are stored by default in a [Qdrant database](https://qdrant.tech).  If you have defined the `qdrant_local_db` configuration option it will use a (very slow) local database.  Otherwise you should define the `qdrant_url` option pointint to the endpoint of your database, and the environment variable `QDRANT_API_KEY`.

### Running the Slack bot

To run the Slack bot, create your own app using the manifest (in the code). Then install your app in the workspace, and add the following environment variables:

- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`

Then run `aword/slackbot.py`.

### Ingesting data

To get data from Slack (through [crowd.dev](https://crowd.dev)), add the following environment variables:

- `CROWDDEV_TENANT_ID`
- `CROWDDEV_API_KEY`

and run `aword/sources/crowddev.py`.

To get data from Notion, add the following environment variable:

- `NOTION_API_KEY`

and run `aword/sources/notion.py`.

To get data from [Linear](https://linear.app), add the following environment variable:

- `LINEAR_API_KEY`

and run `aword/sources/linear.py`.

## Dev

```
mkdir ~/venv/awo && python -m venv ~/venv/awo
source ~/venv/awo/bin/activate
pip install --upgrade pip
pip install -e .
pip install ".[dev]"
```

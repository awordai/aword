# aWord

aWord is an OpenAI-GPT-powered information retrieval system that embeds pieces of information coming from several sources, stores the embeddings in a vector database, and enables querying based on the embeddings (RAG, Retrieval Augmentation Generation).

It does this with particular care about data handling and compartimentalization, allowing the user to define scopes (like 'confidential', or 'public') and personas (which have access to scopes).

*aWord is pre-release alpha software.*

## Running and configuration

Install with:

```bash
git clone git@github.com:awordai/aword.git
cd aword
pip install -e .
```

aWord is intended to be run as a library, but you can also call it from the command line:

```bash
aword -h
```

```plaintext
usage: aword [-h] [-v] [-d] [-e ENVIRONMENT] [-L ERROR_LOGS_DIR] [-C CONFIG_DIR]
             {app,notion,local,chat,ask,cache,store} ...

Entry point to aWord.

options:
  -h, --help            show this help message and exit
  -v, --verbose         Enable info logs. (default: False)
  -d, --debug           Enable debug logs. (default: False)
  -e ENVIRONMENT, --environment ENVIRONMENT
                        Environment name. If set, for example, to dev the file
                        .env.dev will be loaded if existing. If not set the file .env
                        will be loaded if existing. (default: )
  -L ERROR_LOGS_DIR, --error-logs-dir ERROR_LOGS_DIR
                        Directory for the error logs. (default: logs)
  -C CONFIG_DIR, --config-dir CONFIG_DIR
                        Directory with the configuration files. If present it will not
                        attempt to search for them in the default places (the value of
                        the AWORD_CONFIG_DIR environment variable and the ~/.aword
                        folder) (default: None)

Commands:
  {app,notion,local,chat,ask,cache,store}
    app                 Main application.
    notion              Parse and cache notion pages.
    local               Parse and cache local documents.
    chat                Have conversations with a persona.
    ask                 Ask questions to a respondent.
    cache               Query the cache.
    store
```

```bash
aword app -h
```

```plaintext
usage: aword app [-h] [--embed-cache]

options:
  -h, --help     show this help message and exit
  --embed-cache  Chunks and embeds the unembedded segments in the cache, and stores
                 them in the vector database (default: False)
```

### Environment

aWord tries to load environment variables from `.env`, or `.env.test` when run by `pytest`.  When run from the command line the `--environment` option names an environment to load. For example,

```bash
aword --environment dev
```

will try to load `.env.dev`.

The relevant environment variables are:

```sh
AWORD_TESTING
AWORD_PRODUCTION
AWORD_CONFIG_DIR
AWORD_OPENAI_API_KEY
AWORD_QDRANT_API_KEY

AWORD_CROWDDEV_TENANT_ID
AWORD_CROWDDEV_API_KEY
AWORD_LINEAR_API_KEY
AWORD_NOTION_API_KEY
AWORD_SLACK_BOT_TOKEN
AWORD_SLACK_APP_TOKEN
```

### Main configuration options

Configuration files are searched for in the following directories, in this order:

1. The current directory;
2. The content of the `AWORD_CONFIG_DIR` environment variable;
3. `~/.aword/`

The file `config.ini` file defines the following configuration options:

```ini
[cache]
provider = edge
add_summaries = true
db_file = res/dev/cache.db

[vector]
provider = qdrant
local_db = res/dev/local.qdrant
collection_name = test-collection

# if qdrant_url is defined we'll use it
# url =

[embedding]
model_name = text-embedding-ada-002
embedding_chunk_size = 400
```

## Functionality overview

The functionality can be divided in two parts: ingestion, and data use.  Ingestion should happen regularly, possibly triggered by a cron.  It includes:

- Fetch information from sources (like notion, or local directories).  Each source has source units (a notion page, a file in a local directory).  Each source unit has a scope (like "confidential", or "customer_support"), a context (like "historical", or "reference") and one or more categories.
- Parse the information into chunks.
- Store the chunks and their provenance in a cache database.  The current version implements an edge cache based in SQLite.  The chunks are stored in a way that enables incremental fetching and parsing.
- Create embeddings for the chunks and store them in a vector database.  The current implementation uses Qdrant.  Each chunk in the vector database knows the source unit from which it has been extracted, as well as its scope, context and categories.  When a source unit changes all its chunks are deleted from the vector database and new embeddings are stored.

Data use includes:

- Retrieve the relevant chunks to answer a query from the vector database, possibly given a scope, a context, and some categories.
- Use the retrieved chunks as the background information to query a model, together with input from the user.

### Personas

A persona is an entity that has access to the chunks belonging to a set of scopes for use as background information, and can use them to query the model with customer questions.  You can define, for example, a persona that can only access the "customer_support" chunks.  This persona can safely answer customer questions without leaking data tagged as "confidential".

You can define personas with a json file `personas.json` in a config directory:

```json
{
  "support": {
    "system_prompt": "You are a helpful customer support agent.  Your job is to help customers, answering their questions. Your answers should be polite and suscinct, and they should not contain more than ${answer_words} words.",
    "user_prompt_preface": "Summarize the following text:\n\n",
    "scopes": ["customer_support"],
    "model_name": "gpt-3.5-turbo",
    "answer_words": 128,
    "provider": "openai"
  },
  "expert": {
    "system_prompt_file": "system-prompt-expert.txt",
    "user_prompt_preface": "",
    "scopes": ["confidential", "customer_support"],
    "model_name": "gpt-4",
    "provider": "openai"
  }
}
```

Once a persona is defined you can ask questions:

```bash
aword --environment dev --verbose chat @expert "how do inkjet printers work?"
```

```plaintext
aword --environment dev -v chat @expert "how do inkjet printers work?"
app:: Loading env file .env.dev
app:: Found config file res/dev/personas.json
app:: Found config file res/dev/system-prompt-expert.txt
app:: Found config file res/dev/models.json
persona:: ask @expert: how do inkjet printers work?
I'm sorry, but I'm unable to provide an answer to this question as there's no provided background information related to the functioning of inkjet printers.
```

This is good, because the system prompt included

```plaintext
Give an answer as specific as possible, deriving it from the background, and being careful not to say anything that cannot be inferred from the background.
```

GPT-4 respects this, GPT-3.5 does not.


### Configuration of sources

aWord can import data from several sources, defined in a `sources.json` configuration file.

The sources definition looks like this:

```json
{
  "notion": {
    "databases": [
      {
        "id": "482...",
        "doc": "Shared notes",
        "categories": ["R&D"],
        "scope": "confidential",
        "context": "historical"
     },
      {
        "id": "fe8...",
        "doc": "User research notes",
        "categories": ["R&D"],
        "scope": "confidential",
        "context": "historical"
      }
    ],
    "pages": [
      {
        "id": "c50...",
        "doc": "Project A documentation",
        "scope": "confidential"
        "context": "reference"
      },
      {
        "id": "e487...",
        "doc": "Meeting notes",
        "scope": "confidential",
        "context": "historical"
      }
    ]
  },
  "local": [
    {
      "directory": "res/test/local",
      "extensions": ["md", "org", "txt"],
      "categories": ["wands"],
      "scope": "confidential",
      "context": "reference"
      "author": "unknown"
    },
    {
      "directory": "res/test/local/short",
      "author": "John Doe",
      "scope": "public",
      "context": "historical"
    }
  ]
}

```

### Access to the OpenAI API

The environment variable `AWORD_OPENAI_API_KEY` is always required.

### Embedding database API

Embeddings are stored by default in a [Qdrant database](https://qdrant.tech).  If you have defined the `qdrant_local_db` configuration option it will use a (very slow) local database.  Otherwise you should define the `qdrant_url` option pointint to the endpoint of your database, and the environment variable `AWORD_QDRANT_API_KEY`.

## Dev

```
mkdir ~/venv/awo && python -m venv ~/venv/awo
source ~/venv/awo/bin/activate
pip install --upgrade pip
pip install -e .
pip install ".[dev]"
```

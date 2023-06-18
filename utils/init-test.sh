rm res/test/last-seen.json
rm -rf res/test/local.qdrant
AWORD_CONFIG=res/test/config.ini python utils/qdrant-init.py
AWORD_CONFIG=res/test/config.ini AWORD_SOURCES_CONFIG=res/test/sources.json python aword/sources/local.py

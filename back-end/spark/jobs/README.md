Spark jobs live here.

Planned streaming jobs:
- Topic matching: `reddit.*.raw` -> `reddit.topic.matched`
- Aggregations/trends: `reddit.topic.matched` -> `reddit.topic.metrics`, `reddit.topic.trends`
- Sentiment (lexicon-based): `reddit.topic.matched` -> `reddit.topic.sentiment`
- Global viral topics: `reddit.*.raw` -> `reddit.global.trends`


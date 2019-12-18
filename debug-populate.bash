#! /usr/bin/env bash

echo "curl  'http://localhost:8888/topic?topic=search-web&description=Search+a+sebsite'" && curl  'http://localhost:8888/topic?topic=search-web&description=Search+a+sebsite' && \
echo "curl  'http://localhost:8888/topic?topic=search-host&description=Search+a+host'" && curl  'http://localhost:8888/topic?topic=search-host&description=Search+a+host' && \
echo "curl  'http://localhost:8888/topic?topic=aggregate&description=Aggregate+data'" && curl  'http://localhost:8888/topic?topic=aggregate&description=Aggregate+data' && \
echo "curl  'http://localhost:8888/subscribe?topic=aggregate'" && curl  'http://localhost:8888/subscribe?topic=aggregate' && \
echo "curl  'http://localhost:8888/subscribe?topic=aggregate'" && curl  'http://localhost:8888/subscribe?topic=aggregate' && \
echo "curl  'http://localhost:8888/subscribe?topic=search-api'" && curl  'http://localhost:8888/subscribe?topic=search-api' && \
echo "curl  'http://localhost:8888/topic?topic=search-api&description=Search+an+API'" && curl  'http://localhost:8888/topic?topic=search-api&description=Search+an+API' && \
echo "curl  'http://localhost:8888/subscribe?topic=search-api'" && curl  'http://localhost:8888/subscribe?topic=search-api' && \
echo "curl  'http://localhost:8888/publish?topic=aggregate&message=last+month'" && curl  'http://localhost:8888/publish?topic=aggregate&message=last+month' && \
echo "curl  'http://localhost:8888/publish?topic=search-host&message=127.0.0.1:7000'" && curl  'http://localhost:8888/publish?topic=search-host&message=127.0.0.1:7000' && \
echo "curl  'http://localhost:8888/publish?topic=search-web&message=https://www.google.com'" && curl  'http://localhost:8888/publish?topic=search-web&message=https://www.google.com' && \
echo "curl  'http://localhost:8888/publish?topic=search-web&message=https://www.nytimes.com'" && curl  'http://localhost:8888/publish?topic=search-web&message=https://www.nytimes.com' && \
echo "curl  'http://localhost:8888/publish?topic=search-api&message=https://www.testweb.com/api/v1/entites?eek=1'" && curl  'http://localhost:8888/publish?topic=search-api&message=https://www.testweb.com/api/v1/entites?eek=1' && \
echo "curl  'http://localhost:8888/publish?topic=search-api&message=https://www.testweb.com/api/v1/entites?eek=2'" && curl  'http://localhost:8888/publish?topic=search-api&message=https://www.testweb.com/api/v1/entites?eek=2' && \
echo "curl  'http://localhost:8888/publish?topic=search-api&message=https://www.testweb.com/api/v1/entites?eek=3'" && curl  'http://localhost:8888/publish?topic=search-api&message=https://www.testweb.com/api/v1/entites?eek=3' && \
echo 'curl -X POST  "http://localhost:8888/publish" -H "Content-type: application/json" --data "{\"topic\": \"search-web\", \"message\": \"https://data-sent-from-post.com\"}' && curl -X POST  'http://localhost:8888/publish' -H "Content-type: application/json" --data '{"topic": "search-web", "message": "https://data-sent-from-post.com"}'

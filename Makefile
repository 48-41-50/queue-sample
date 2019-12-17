
.PHONY=network db server clean
DOCKER=docker

DOCKER_NET=queue-net
QUEUE_DB=queue-db
QUEUE_SERVER_IMG=queue-server-img:latest
QUEUE_SERVER=queue-server
QUEUE_CLIENT=queue-client


clean:
	$(DOCKER) kill $(QUEUE_SERVER) $(QUEUE_CLIENT) $(QUEUE_DB) 2>/dev/null || true
	$(DOCKER) rm $(QUEUE_SERVER) $(QUEUE_CLIENT) $(QUEUE_DB) 2>/dev/null || true
	$(DOCKER) rmi $(QUEUE_SERVER_IMG) || true
	$(DOCKER) network rm $(DOCKER_NET) || true

network:
	$(DOCKER) network create $(DOCKER_NET) 2>/dev/null || true

db: network
	$(DOCKER) kill $(QUEUE_DB) || true
	$(DOCKER) rm $(QUEUE_DB) || true
	$(DOCKER) run \
	          --rm \
	          --detach \
	          --name $(QUEUE_DB) \
	          -e POSTGRES_USER=queues \
	          -e POSTGRES_DB=queues \
	          --network $(DOCKER_NET) \
	          -v $(PWD)/init:/docker-entrypoint-initdb.d \
	          -p 5432:5432 \
	          postgres:12

server: network db
	$(DOCKER) kill $(QUEUE_SERVER) || true
	$(DOCKER) rm $(QUEUE_SERVER) || true
	$(DOCKER) build -t $(QUEUE_SERVER_IMG) ./http
	$(DOCKER) run \
	          --rm --detach \
	          --name $(QUEUE_SERVER) \
	          --network $(DOCKER_NET) \
	          -p "8888:8888" \
	          $(QUEUE_SERVER_IMG) python /opt/server/queue_server.py



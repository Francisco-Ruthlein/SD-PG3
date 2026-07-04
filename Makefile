.PHONY: build run stop clean logs status

build:
	docker-compose build

run:
	docker-compose up -d

stop:
	docker-compose down

clean:
	docker-compose down -v
	docker system prune -f

logs:
	docker-compose logs -f

status:
	@for port in 8081 8082 8083 8084 8085; do \
		echo "=== Nodo en puerto $$port ==="; \
		curl -s http://localhost:$$port/estado || echo "No responde"; \
	done

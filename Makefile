.PHONY: extract normalize load pipeline postgres up down clean

# -------------------------------------------------------------------
# Atalhos úteis
# -------------------------------------------------------------------

extract:
	@echo ">>> Executando EXTRACTION..."
	@docker compose -f docker/docker-compose.yml run --rm extract

normalize:
	@echo ">>> Executando NORMALIZATION..."
	@docker compose -f docker/docker-compose.yml run --rm normalize

load:
	@echo ">>> Executando LOAD..."
	@docker compose -f docker/docker-compose.yml run --rm load

pipeline:
	@make extract
	@make normalize
	@make load

up:
	@docker compose -f docker/docker-compose.yml up -d postgres

down:
	@docker compose -f docker/docker-compose.yml down

clean:
	@rm -rf output/csv/*.csv
	@rm -rf output/norm_csv/*.csv
	@echo "[OK] Limpeza concluída."
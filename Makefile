.PHONY: airflow-up
airflow-up:
	cd airflow && docker compose --profile pgadmin up -d

airflow-down:
	cd airflow && docker compose down

airflow-clean:
	cd airflow && docker compose down --volumes --remove-orphans
	rm -rf db_data/postgres_data/* 
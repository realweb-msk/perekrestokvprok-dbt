#!/bin/sh

# Команды, который выполняет Cloud Run

dbt deps --profiles-dir .
dbt debug --target dev --profiles-dir .
dbt debug --target prod --profiles-dir .
dbt run --target prod --profiles-dir .
dbt test --target prod --profiles-dir .
dbt clean --profiles-dir .

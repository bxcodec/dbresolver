# Exporting bin folder to the path for makefile
export PATH   := $(PWD)/bin:$(PATH)
# Default Shell
export SHELL  := bash
# Type of OS: Linux or Darwin.
export OSTYPE := $(shell uname -s)

export MUSL   := $(shell [ -x /sbin/apk ] &&  echo "-tags musl" || echo "")

ifeq ($(OSTYPE),Darwin)
    export MallocNanoZone=0
endif

define github_url
    https://github.com/$(GITHUB_REPO)/releases/download/v$(VERSION)/$(ARCHIVE)
endef

# creates a directory bin.
bin:
	@ mkdir -p $@

# ~~~ Tools ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~ [migrate] ~~~ https://github.com/golang-migrate/migrate ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MIGRATE := $(shell command -v migrate || echo "bin/migrate")
migrate: bin/migrate ## Install migrate (database migration)

bin/migrate: VERSION := 4.14.1
bin/migrate: GITHUB_REPO  := golang-migrate/migrate
bin/migrate: ARCHIVE := migrate.$(OSTYPE)-amd64.tar.gz
bin/migrate: bin
	@ printf "Install migrate... "
	@ curl -Ls $(call github_url) | tar -zOxf - ./migrate.$(shell echo $(OSTYPE) | tr A-Z a-z)-amd64 > $@ && chmod +x $@
	@ echo "done."


# ~~~ Database Migrations ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= my_password
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 5432
POSTGRES_DATABASE ?= my_database


PG_DSN := "postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DATABASE)?sslmode=disable"

migrate-up: $(MIGRATE) ## Apply all (or N up) migrations.
	@ read -p "How many migration you wants to perform (default value: [all]): " N; \
	migrate  -database $(PG_DSN) -path=internal/postgres/migrations up $${N}
# if you encounter the dirty version, fix the error, then use the below command
# migrate -database $(PG_DSN) -path=internal/postgres/migrations force previous_version up

.PHONY: migrate-down
migrate-down: $(MIGRATE) ## Apply all (or N down) migrations.
	@ read -p "How many migration you wants to perform (default value: [all]): " N; \
	migrate  -database $(PG_DSN) -path=internal/postgres/migrations down $${N}

.PHONY: migrate-drop
migrate-drop: $(MIGRATE) ## Drop everything inside the database.
	migrate  -database $(PG_DSN) -path=internal/postgres/migrations drop

.PHONY: migrate-create
migrate-create: $(MIGRATE) ## Create a set of up/down migrations with a specified name.
	@ read -p "Please provide name for the migration: " Name; \
	migrate create -ext sql -dir internal/postgres/migrations $${Name}

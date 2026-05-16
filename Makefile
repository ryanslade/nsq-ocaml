.PHONY: help build test fmt clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Build the project
	dune build

test: ## Run tests (requires NSQ running, see docker-compose.yml)
	dune runtest --force

fmt: ## Format source files
	dune build @fmt --auto-promote

clean: ## Remove build artifacts
	dune clean

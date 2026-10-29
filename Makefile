SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

GO          ?= go
GOLANGCI    ?= golangci-lint
COVER_MIN   ?= 70
BIN         := bin

# Tailwind standalone CLI. Checksums come from the release's sha256sums.txt.
TAILWIND_VERSION := v4.3.3
TAILWIND         := $(BIN)/tailwindcss-$(TAILWIND_VERSION)
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
  TW_OS := macos
else
  TW_OS := linux
endif
ifneq (,$(filter $(UNAME_M),arm64 aarch64))
  TW_ARCH := arm64
else
  TW_ARCH := x64
endif
TW_ASSET := tailwindcss-$(TW_OS)-$(TW_ARCH)
TW_SHA_linux-x64   := dc61b3ac6b8c9ca874c0cc4c57b2409791a64c5540404ca5f5367360babc313a
TW_SHA_linux-arm64 := 55fd0b241214eff3de1e8ee4f22796662f2d2e7a49bcfca7477cfd0bac398195
TW_SHA_macos-x64   := 7922e0953f2110c05976e3bf58f14e643d90427575e766b7d433f5f80cbee7e1
TW_SHA_macos-arm64 := cdf646702987a743464dff4d9c60fd4480d1c1e73dd819a9a67f1078815dce9d
TW_SHA := $(TW_SHA_$(TW_OS)-$(TW_ARCH))

# Files that `make generate` produces and that must be committed.
GENERATED := web/static/app.css $(shell find . -name '*_templ.go' -not -path './bin/*')

.PHONY: all build generate templ css check-generated lint sec test cover check vuln dev clean

all: check build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o takt ./cmd/takt

generate: templ css

templ:
	$(GO) tool templ generate

css: $(TAILWIND)
	$(TAILWIND) -i web/styles/input.css -o web/static/app.css --minify

$(TAILWIND):
	@mkdir -p $(BIN)
	curl -sSfL -o $@.tmp https://github.com/tailwindlabs/tailwindcss/releases/download/$(TAILWIND_VERSION)/$(TW_ASSET)
	echo "$(TW_SHA)  $@.tmp" | sha256sum -c -
	chmod +x $@.tmp
	mv $@.tmp $@

# Fails when generated files differ from what is committed.
check-generated: generate
	@if ! git diff --quiet -- $(GENERATED) web/static; then \
		echo "generated files are stale, commit the output of 'make generate':"; \
		git diff --stat -- $(GENERATED) web/static; \
		exit 1; \
	fi

lint:
	$(GOLANGCI) run ./...

sec:
	$(GO) tool gosec -quiet -exclude-generated ./...

test:
	$(GO) test -race ./...

# Coverage across all packages, excluding generated templ code and main packages.
cover:
	$(GO) test -race -covermode=atomic -coverpkg=./... -coverprofile=coverage.out ./...
	grep -v -E '_templ\.go:|/cmd/|/examples/' coverage.out > coverage.filtered.out
	@total=$$($(GO) tool cover -func=coverage.filtered.out | awk '/^total:/ {sub("%","",$$3); print $$3}'); \
	echo "total coverage: $$total% (min $(COVER_MIN)%)"; \
	awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN { exit (t+0 >= m+0) ? 0 : 1 }' || \
		{ echo "coverage below $(COVER_MIN)%"; exit 1; }

check: check-generated lint sec cover

vuln:
	$(GO) tool govulncheck ./...

dev:
	$(GO) run ./cmd/takt dev

clean:
	rm -rf $(BIN) takt coverage.out coverage.filtered.out

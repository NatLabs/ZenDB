.PHONY: test bench docs check test-all test-replica kill-dfx

check:
	find bench -type f -name 'txs.bench.mo' -print0 | \
	xargs -0 $(shell mops toolchain bin moc) -r $(shell mops sources) -Werror -wasi-system-api

docs:
	$(MocvPath)/mo-doc
	$(MocvPath)/mo-doc --format plain

bench:
	mops bench  --gc incremental%

test:
	mops test 

test-replica:
	mops test --mode replica --replica dfx .replica

# Create temp directory for pipes if needed
PIPE_DIR := $(shell mkdir -p ./tests/.pipes)

test-all:
	@mkfifo ./tests/.pipes/pipe1 ./tests/.pipes/pipe2 2>/dev/null || true
	@mops test > ./tests/.pipes/pipe1 & \
	mops test --mode replica --replica dfx .replica > ./tests/.pipes/pipe2 & \
	cat ./tests/.pipes/pipe1 && \
	cat ./tests/.pipes/pipe2; \
	wait; \
	rm -f -r ./tests/.pipes/pipe1 ./tests/.pipes/pipe2 ./tests/.pipes

kill-dfx:
	@echo "Found these processes:"
	@ps aux | grep -E 'dfx' | grep -v grep
	@echo "Killing processes..."
	@ps aux | grep -E 'dfx' | grep -v grep | awk '{print $$2}' | xargs kill -9 2>/dev/null || true
	@echo "Done"
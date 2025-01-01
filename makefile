.PHONY: test bench docs check test-all test-replica

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
PIPE_DIR := $(shell mkdir -p .pipes)

test-all:
	@mkfifo .pipes/pipe1 .pipes/pipe2 2>/dev/null || true
	@mops test > .pipes/pipe1 & \
	mops test --mode replica --replica dfx .replica > .pipes/pipe2 & \
	cat .pipes/pipe1 && \
	cat .pipes/pipe2; \
	wait; \
	rm -f -r .pipes/pipe1 .pipes/pipe2 .pipes
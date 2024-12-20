.PHONY: test bench docs check

test:
	mops test

check:
	find bench -type f -name 'txs.bench.mo' -print0 | \
	xargs -0 $(shell mops toolchain bin moc) -r $(shell mops sources) -Werror -wasi-system-api

docs:
	$(MocvPath)/mo-doc
	$(MocvPath)/mo-doc --format plain

bench:
	mops bench  --gc incremental%


#!/usr/bin/env zx
const update_backend_canister_in_playground = async () => {
    await $`dfx deploy backend --playground`;
};

// await update_backend_canister_in_playground();
const prevent_playground_timeout = setInterval(
    update_backend_canister_in_playground,
    1000 * 60 * 15,
);

const parse_num = (str) => parseInt(str.replace('_', ''));

const BATCH_SIZE = 2_000;
console.log(argv);
const start = argv.start || 0;

if (start % BATCH_SIZE !== 0) {
    throw new Error(
        'start (' + start + ') must be a multiple of BATCH_SIZE: ' + BATCH_SIZE,
    );
}

if (argv.length && argv.length % BATCH_SIZE !== 0) {
    throw new Error(
        'length (' +
            argv.length +
            ') must be a multiple of BATCH_SIZE: ' +
            BATCH_SIZE,
    );
}

const end = argv.length ? start + argv.length : BATCH_SIZE;

let curr = start;

while (curr < end) {
    await $`dfx canister call --playground backend pull_blocks_into_db '(${curr}, ${BATCH_SIZE})'`;

    console.log(`stored blocks at ${curr / BATCH_SIZE} batch`);

    curr += BATCH_SIZE;
}

clearInterval(prevent_playground_timeout);

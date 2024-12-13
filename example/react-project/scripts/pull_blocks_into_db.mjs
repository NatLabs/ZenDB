#!/usr/bin/env zx

const get_db_size = async () => {
    let raw_db_size = (
        await $`dfx canister --ic call backend get_db_size`
    ).stdout
        .replace('\n', '')
        .replace(' ', '')
        .replace('_', '')
        .slice(1, -1)
        .split(':')[0];

    let db_size = parseInt(raw_db_size);

    console.log(`db_size: ${db_size}`);

    if (isNaN(db_size)) {
        console.log('db_size is NaN');
        exit(1);
    }

    return db_size;
};

const parse_num = (str) => parseInt(str.replace('_', ''));

const BATCH_SIZE = 10_000;
console.log(argv);
const start = argv.start || (await get_db_size());

// if (start % BATCH_SIZE !== 0) {
//     throw new Error(
//         'start (' + start + ') must be a multiple of BATCH_SIZE: ' + BATCH_SIZE,
//     );
// }

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
    await $`dfx canister call --ic backend pull_blocks_into_db '(${curr}, ${BATCH_SIZE})'`;

    console.log(
        `stored ${BATCH_SIZE} blocks from ${curr} to ${curr + BATCH_SIZE}`,
    );

    curr += BATCH_SIZE;
}

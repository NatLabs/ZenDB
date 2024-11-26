import fs from 'fs';
import { parallelLimit, asyncify } from 'async';

const { start_batch } = argv;

const STORED_BATCH_SIZE = 10_000;

const TXS_DIR = 'icp_blocks';

await $`rm -rvf ./${TXS_DIR}/*k.plain`;
const total_batches = fs.readdirSync(`./${TXS_DIR}`).length;

const get_num_stored_batches = async () => {
    let raw_num_txs = (await $`dfx canister call backend get_db_size`).stdout
        .replace('\n', '')
        .replace(' ', '')
        .replace('_', '')
        .slice(1, -1)
        .split(':')[0];

    let num_txs = parseInt(raw_num_txs);

    console.log(`num_txs: ${num_txs}`);

    if (isNaN(num_txs)) {
        console.log('num_txs is NaN');
        exit(1);
    }

    return num_txs / STORED_BATCH_SIZE;
};

let num_stored_batches = start_batch
    ? start_batch - 1
    : await get_num_stored_batches();

console.log(`num_stored_batches: ${num_stored_batches}`);

const upload_batch = async (batch) => {
    // decompress file
    await $`gunzip -c ./${TXS_DIR}/${batch * 10}k.gz > ./${TXS_DIR}/${
        batch * 10
    }k.plain`;

    // upload decompressed file (same file name without .gz)
    await $`dfx canister call backend upload_blocks --argument-file ./${TXS_DIR}/${
        batch * 10
    }k.plain`;

    // remove decompressed file
    await $`rm ./${TXS_DIR}/${batch * 10}k.plain`;

    console.log(`stored batch ${batch}`);
};

const BATCHES_AT_ONCE = 3;

const total_parallel_batches = total_batches - num_stored_batches;
console.log('total parallel batches: ', total_parallel_batches);

const batch_upload_tasks = Array.from({ length: total_parallel_batches })
    .map((_, i) => num_stored_batches + i + 1)
    .map((n) => async () => upload_batch(n));

parallelLimit(batch_upload_tasks, BATCHES_AT_ONCE, () => {
    console.log('done uploading');
});

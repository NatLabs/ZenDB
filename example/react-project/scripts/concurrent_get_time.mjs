#!/usr/bin/env zx

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// await sleep(1000);
await Promise.all(
    Array.from({ length: 5 }).map(async (_, i) => {
        const res =
            await $`dfx canister call backend --playground read_and_update`;

        console.log(`[${i}] ${res}`);
    }),
);

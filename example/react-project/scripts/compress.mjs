import { readdirSync } from 'fs';

let TXS_DIR = 'icp_blocks';

const files = readdirSync(`./${TXS_DIR}`);

for (const file of files) {
    if (!file.endsWith('.did')) continue;

    await $`rm ./icp_blocks/${file}`;
}

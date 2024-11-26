#!/usr/bin/env zx
import { parse_json, print_test_result, entitle } from "./utils.mjs";
import chalk from "chalk";

$.verbose = false; // set to true so that we can see the deployment logs and respond if ther are input prompts
const is_deploy = "deploy" in argv ? argv.deploy : true;

if (is_deploy) {
  entitle("Deploying asset-test canister...");
  await $`dfx deploy asset-test`;
}

await $`dfx ledger fabricate-cycles --canister asset-test`;

entitle("Adding asset-test canister as a controller...");
// Add the asset-test canister as a controller of itself
await $`dfx canister update-settings asset-test --add-controller $(dfx canister id asset-test)`;

$.verbose = false;
entitle("Retrieving test details...");
const test_details_raw_test =
  await $`dfx canister call asset-test get_test_details`;

const test_details = parse_json(test_details_raw_test.stdout);

let passed = 0;
let failed = 0;

const test_prefix = argv?.test || "";

entitle("\nRunning tests...\n");

for (const { is_query, name } of test_details) {
  if (!is_deploy && !name.startsWith(test_prefix)) {
    continue;
  }

  let args = `("${name}")`;

  let test = {};

  try {
    let raw_test = is_query
      ? await $`dfx canister call asset-test run_query_test ${args}`
      : await $`dfx canister call asset-test run_test  ${args}`;

    test = parse_json(raw_test.stdout);
  } catch (e) {
    let raw_test =
      await $`dfx canister call asset-test get_test_result ${args}`;

    test = parse_json(raw_test.stdout);
    test.result = false;
    test.print_log.push(`${chalk.red("[Canister Error] ")} ${e.stderr}`);
  }

  print_test_result(test);

  if (test.result) {
    passed += 1;
  } else {
    failed += 1;
  }
}

console.log("\n" + `Passed: ${passed}, Failed: ${failed}`);

if (failed === 0) {
  console.log(chalk.bold.green("All tests passed!"));
} else {
  process.exit(1);
}

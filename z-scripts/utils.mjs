import chalk from "chalk";

export function extract_text_from_brackets(str, first, last) {
  const start = str.indexOf(first) + first.length;
  const end = str.lastIndexOf(last) + 1 - last.length;

  if (start === -1 || end === -1) {
    return str;
  }

  return str.slice(start, end);
}

export function trim_end(str, char) {
  return str.replace(new RegExp(char + "*$"), "");
}

export function parse_json(str) {
  let formatted = extract_text_from_brackets(str, "(", ")").trim();

  formatted = trim_end(formatted, ",");

  formatted = extract_text_from_brackets(formatted, '"', '"');

  let unquoted = formatted
    // Handle escaped quotes
    .replace(/\\"/g, '"')
    // Handle quadruple-escaped quotes
    .replace(/\\"/g, '"')
    // Handle lone double-backslashes (like \\\\43) -> \43
    .replace(/\\{4}(?=\d)/g, "\\");

  let json = {};

  try {
    json = JSON.parse(unquoted);
  } catch (e) {
    console.log({ str, unquoted });
    throw new Error("Failed to parse JSON " + e);
  }

  return json;
}

export const entitle = (str) =>
  console.log("\n" + chalk.bold.underline(str) + "\n");

export const print_test_result = (test) => {
  console.log(
    test.result
      ? ` ✅ ${chalk.green(test.name)}`
      : ` ${chalk.bold.red("❌")} ${chalk.bold.red(test.name)}`
  );

  for (const print_statement of test.print_log) {
    console.log("\t" + chalk.rgb(210, 220, 220)("➥  " + print_statement));
  }
};

# PowerSim AI Assistant Safety

## Data privacy boundary

Do not commit private GSE data, `project_data`, `out` folders, generated results, or Excel workbooks. Any private data used in a session must remain within the authorized workspace.

## Prohibited actions

The embedded assistant must not:

- expose API keys, credentials, tokens, secrets, or environment variables;
- run arbitrary shell commands;
- delete files or folders;
- perform git operations such as branch edits, commits, pushes, merges, or branch deletion;
- upload private GSE data externally;
- overwrite source files from chat;
- modify GitHub issues or pull requests from embedded chat.

## Confirmation requirements

Explicit confirmation is required before solver execution, adequacy runs, input JSON edits, file writes, or report exports.

## Validation caveat

PowerSim AI explanations and model outputs are planning aids only. They are not operationally certified and should be validated against authorized GSE source data and planning standards before decisions.

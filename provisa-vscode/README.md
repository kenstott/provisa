# Provisa for VS Code

Write a Node or Python code source for Provisa and see the tables and commands it becomes as you type.

- **What Provisa would serve.** The Provisa view in the Explorer lists each function Provisa would serve as a table or a command, with the parameters and columns it infers. It runs Provisa's own reader on your folder, before anything is registered.
- **Marks in the editor.** Each docstring or JSDoc tag, each `provisa-sdk` decorator or `@provisa/sdk` wrapper, and each Python `Annotated[...]` hint is marked where you wrote it. Hover to see what Provisa makes of the function.
- **Dev mode.** **Provisa: Run in Dev Mode** registers a code source that reads your folder in place on a Provisa instance, so the next query reads your last save.
- **Build image.** **Provisa: Build Image** builds the source's image through the instance, the same build the Provisa UI runs, and copies the compose snippet to your clipboard.

The extension never changes your model, and it has no query editor. For GraphQL, SQL and Cypher, point the extensions you already use at Provisa. The [VS Code page](https://provisa.dev/docs/vscode/) of the Provisa docs says how.

## Settings

| Setting | What it holds |
|---|---|
| `provisa.cli` | The Provisa command, as a list. Default `["provisa"]`. |
| `provisa.python` | The interpreter a Python folder is read under. Empty: the folder's `.venv/bin/python`. |
| `provisa.instances` | The instances you run code on, each `{ "name", "url", "org" }`. |

Sign in to an instance with **Provisa: Sign In** and a personal access token. VS Code keeps the token in its secret storage.

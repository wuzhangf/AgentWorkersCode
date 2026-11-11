# Scenario: The Office — three real agents walk in and work

This is the human-readable script that `plan.json` was compiled from.
Edit the plan directly, or rewrite this and recompile by hand — replay is
deliberately deterministic (no model in the loop at runtime).

## Beats

1. **Narrate** — a small intranet repo was just prepared on this machine.
2. **Dispatch "inventory"** — list files, read README, write INVENTORY.md.
3. **Dispatch "treasure hunt"** — grep for `quasar`, write FOUND.md.
4. **Dispatch "doc writer"** — read docs/, write DOCS-SUMMARY.md.
5. **Wait** until all three finish — narrate that work happened in
   task-owned clones (writeScope), the real repo is untouched.
6. **Show outputs** — print the produced .md files.
7. **Interactive** — the user types natural-language tasks; each becomes
   a new character.

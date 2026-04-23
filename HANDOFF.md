# C Receivers — Implementation Handoff

**Date:** 2026-04-23
**Branch:** `feature/c-receivers` (worktree at `.worktrees/c-receivers/`)
**Plan:** [docs/2026-04-08-c-receivers-plan.md](docs/2026-04-08-c-receivers-plan.md)
**Spec:** [docs/2026-04-08-c-receivers-design.md](docs/2026-04-08-c-receivers-design.md)

## Status: Tasks 1–6 of 12 complete, all committed

The shared `c/common/` library is fully built and tested. The two binaries (`c/kernel-receiver/` and `c/xdp-receiver/`) and the eBPF program are not yet started.

## What's done

| Task | Status | Notes |
|------|--------|-------|
| 1. Scaffold + tomlc99 + NOTICE | ✅ | Fixed: tomlc99 is MIT-licensed (was incorrectly labeled public domain in spec, plan, and NOTICE) |
| 2. Shred parsing module | ✅ | 11 tests passing. Added `_Static_assert` on header size + tests for resigned merkle variants (0xb0, 0x70) |
| 3. Stats module | ✅ | 12 tests passing. Fixed: counter divergence on dropped old slots; abort on calloc failure; added coverage tests |
| 4. Config module | ✅ | 5 tests passing. Hardened: NULL path guard, positive-integer validation, strict enums, strtol replaces atoi |
| 5. Display log module | ✅ | Compile-only verification (no unit tests for IO) |
| 6. Display TUI module (ncurses) | ✅ | Compile-only verification (visual, can't unit test) |

All work used the subagent-driven loop: implementer → spec review → quality review → fix → re-review.

## What's left

| Task | Notes |
|------|-------|
| 7. Kernel receiver binary | Makefile, `main.c`, `receiver.c`, config.example.toml. **Important:** declare `static stats_t stats;` not stack-local — the embedded `rate_window[16384]` is ~256KB |
| 8. XDP eBPF program (`bpf/xdp_filter.c`) | **Linux only** — needs `clang -target bpf` + libbpf headers |
| 9. XDP loader module (`xdp.c/h`) | **Linux only** — needs libbpf |
| 10. AF_XDP receiver + `find_udp_payload` (TDD) | **Linux only** — needs libxdp's `xsk.h`. The `find_udp_payload` helper and its tests can be split out to test cross-platform if desired |
| 11. XDP main + Makefile + config | **Important:** same `static stats_t` reminder as Task 7 |
| 12. Final cleanup + README update | Update top-level `README.md` table to point at `c/kernel-receiver/` and `c/xdp-receiver/` |

## How to resume

1. **From this same machine:** start a new Claude session, `cd` into the worktree (`.worktrees/c-receivers/`), and re-invoke `superpowers:subagent-driven-development`. Point it at the plan file. Tasks 1–6 are checked complete via the commit log; restart from Task 7.

2. **From the Linux machine** (recommended for Tasks 8–11 since they need libbpf/libxdp/clang-bpf):
   ```bash
   git fetch origin
   git checkout feature/c-receivers
   ```
   Then resume with subagent-driven-development from Task 7.

3. **One-off:** Tasks 7 and 12 can be done on macOS (kernel-receiver builds and runs there with a small caveat about `IP_ADD_MEMBERSHIP` on the GRE interface — may need to test on Linux). Tasks 8–11 strictly require Linux.

## Important reminders for whoever picks this up

1. **`static stats_t` in main.c (Tasks 7 and 11)** — `stats_t` embeds a 256KB `rate_window` array. Declaring it stack-local in `main()` works but is poor practice and breaks if `main` is ever moved or if a non-main thread instantiates one. Use `static stats_t stats;` (BSS) or `calloc`.

2. **`config_parse_cli` two-pass pattern** — `main.c` calls it BEFORE `config_load_file` (to get `--config` path) and AFTER (so CLI overrides win over file values). The function resets `optind = 1` so this is safe.

3. **Spec/plan have been corrected for the tomlc99 MIT issue** — both files in `docs/` are accurate now.

4. **The plan file Task 7 main.c snippet uses stack-local `stats_t stats;`** — change this to `static stats_t stats;` when implementing.

## Verifying current state

```bash
cd .worktrees/c-receivers

# All shared module tests
cd c/common
gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. shred.c shred_test.c -o /tmp/t && /tmp/t && rm /tmp/t
gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. stats.c stats_test.c -o /tmp/t && /tmp/t && rm /tmp/t
gcc -O2 -Wall -c toml.c -o /tmp/toml.o && \
  gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. config.c config_test.c /tmp/toml.o -o /tmp/t && \
  /tmp/t && rm /tmp/t /tmp/toml.o

# Display modules compile-only
gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -D_GNU_SOURCE -I. -c display_log.c display_tui.c && \
  rm display_log.o display_tui.o
```

Expected: 11 + 12 + 5 = 28 PASS lines plus clean compile of display modules.

## Working in parallel

The `feature/c-receivers` branch and worktree are isolated from `main`. You can freely:
- Check out `main` in the primary working directory and start unrelated work
- Push `feature/c-receivers` to remote so the Linux machine can pull it
- Leave the worktree intact — it'll be ready when you (or another session) resume

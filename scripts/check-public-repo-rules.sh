#!/usr/bin/env bash
#
# The rules this repository holds because it is public, checked so that they
# cannot regress quietly. Both of these have been got wrong more than once, in
# each case by a fixture copied from a snippet that nobody audited.
#
# Run from the repository root. No toolchain required.
#
# Values only: a comment naming a forbidden range in order to forbid it is not a
# breach, so lines that are comments are skipped.
set -uo pipefail
fail=0

ROOTS=(rust/codec rust/adapter rust/publisher rust/recorder)

# A gate that cannot find what it was told to scan reports nothing and passes,
# which is the same output as a clean tree and the opposite conclusion. A
# renamed directory is the likely way here: it is a refactor nobody would think
# to check this file against.
for root in "${ROOTS[@]}"; do
  if [ ! -d "$root" ]; then
    echo "Scan root $root does not exist: this check is scanning less than it" >&2
    echo "claims to, and a rule that silently covers nothing is worse than none." >&2
    fail=1
  fi
done
[ "$fail" -eq 0 ] || exit 1

scan() { grep -rnE --include=*.rs --include=*.toml "$1" "${ROOTS[@]}" 2>/dev/null \
         | grep -vE ':[0-9]+:[[:space:]]*(//|#)'; }

# Both spellings. Rust writes an address as four arguments far more often than
# as a string — 62 sites here — so a rule that knows only the dotted quad is a
# rule that would have missed every fixture it exists to catch.
octets='[0-9]{1,3}'
sep='[[:space:],]+'
private_dotted='\b(10\.[0-9]+\.[0-9]+\.[0-9]+|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]+\.[0-9]+|192\.168\.[0-9]+\.[0-9]+|239\.[0-9]+\.[0-9]+\.[0-9]+)\b'
private_ctor="Ipv4Addr::new\(${sep}?(10${sep}${octets}${sep}${octets}|172${sep}(1[6-9]|2[0-9]|3[01])${sep}${octets}|192${sep}168${sep}${octets}|239${sep}${octets}${sep}${octets})${sep}${octets}${sep}?\)"

bad=$(scan "${private_dotted}|${private_ctor}" | grep -vE 'forbidden|\.contains\(')
if [ -n "$bad" ]; then
  echo "Addresses outside the documentation ranges. Use RFC 5737 (192.0.2.0/24," >&2
  echo "198.51.100.0/24, 203.0.113.0/24) or MCAST-TEST-NET (233.252.0.0/24):" >&2
  echo "$bad" >&2; fail=1
fi

# grep reads one line at a time, so a constructor split across lines is one the
# address rule above cannot see. Rather than pretend to parse it, the split form
# itself is the breach: written on one line it is checkable, and rustfmt leaves
# it there at these widths anyway.
bad=$(scan 'Ipv4Addr::new\([[:space:]]*$')
if [ -n "$bad" ]; then
  echo "An Ipv4Addr::new spread over several lines is an address this check" >&2
  echo "cannot read. Write it on one line:" >&2
  echo "$bad" >&2; fail=1
fi

bad=$(scan 'marketdata' | grep -vE 'assert|\.contains\(|is not a spelling')
if [ -n "$bad" ]; then
  echo 'The port role token is "mktdata"; "marketdata" is not a spelling this code knows:' >&2
  echo "$bad" >&2; fail=1
fi
exit $fail

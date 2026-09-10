# The upstream-message object format

The second archive shape this crate writes: a venue's own upstream messages,
length-delimited, as the evidence a venue-side observation is derived from.

This document states the layout. `src/upstream.rs` is the implementation and
`tests/upstream_format.rs` holds it to what is written here.

## Why there are two archive shapes

The first shape is pcapng, one Enhanced Packet Block per datagram, and it is
what a capture writes. A capture is a receive path over a socket that observes
datagrams, counts what the handle dropped, and records link headers.

A venue-side recording is none of those three. What arrives where a venue's own
bytes do is a websocket message, a tag-value message on a session, or a response
body — and it has no source address, no destination group, no port role, no
capture-handle drop count, and no wire length distinct from its own. There are
no link headers to record and no datagram boundaries to preserve. Every field a
pcapng block would need is one an upstream recording would have to invent, and
each of those inventions is a value a query cannot tell from a reading: channel
`0` is a channel, port `0` is a port a document can state, and `0.0.0.0` is an
address that reads as *unset* to a person and as a value to a query.

So this shape carries what there is, per message: the bytes exactly as they
arrived, the connection that delivered them, and a receive stamp. Those are
`dz_adapter_core::Payload`'s own fields, because that is what the adapter will
be handed and a second field set here would be a second definition of it.

## Why this is not the normalized-event record encoding

`dz-recorder-relower` compares what a publisher sent against what a venue said,
and it has a record encoding for the **normalized events** either side of that
diff. That encoding is deliberately not reused here, and the reason is not
convenience.

**It sits downstream of a venue's decode.** An archive of normalized events is
an archive of what one build of one adapter made of the venue's bytes. A mapping
defect found next month cannot be re-examined against it, because the evidence
has already been through the thing under suspicion: the events it holds are the
output of the mapping whose correctness is the question. Re-running a corrected
adapter over it would produce the same rows, and produce them from the same
mistake.

Keeping raw bytes is what makes that re-derivation possible, which is the whole
reason the offline re-lowering exists on the datagram side. The evidence has to
be what the venue sent.

The two coexist and neither substitutes for the other:

| | What it holds | What it is for |
|---|---|---|
| **This format** | The venue's upstream bytes, verbatim | Evidence: re-derivable with a corrected adapter, and the thing a row can be traced back to |
| The re-lowering's record encoding | Normalized events | The reference a re-lowering diffs against, on both sides of the comparison |

A reader that wants the second from an object of the first runs the derivation.
There is no path in the other direction, and that asymmetry is the point.

## What is not written twice

Four questions have one answer each in this crate, and this format asks none of
them again:

| Question | Where it is answered |
|---|---|
| When does a segment rotate? | `rotate::RotationPolicy` — size or age, whichever comes first. `UpstreamSegmentWriter` does not rotate: it accounts for the bytes it has written and states the window it covers, and the venue's own binary reads that count against this policy |
| How is an object compressed and digested? | `compress::seal` — zstd with its own per-frame checksum on, hashing the bytes that land |
| Where does an object land? | `object_key::object_key` — the Hive-partitioned key, with the site and the recorder in it |
| What is a re-derivation idempotent on? | `(object key, sha256)`, which the manifest states |

A second set of answers is how two archives in one repository come to disagree
about retention while both files still read as though they agree. What this
format adds is the record layout and the object's extension, and nothing else.

## The layout

Little-endian throughout, which is the byte order the datagram header on the
multicast side already uses. No padding and no alignment: every field is read at
a computed offset.

### The object header

Written once, when the segment is opened.

| Offset | Width | Field |
|---|---|---|
| 0 | 8 | Magic, `DZUPSTRM` |
| 8 | 2 | Format version (`u16`), currently `1` |
| 10 | 2 | Connection count (`u16`), at most 256 |

Then, for each connection, in the order records index them:

| Width | Field |
|---|---|
| 2 | Name length in bytes (`u16`) |
| 1 | Receive-stamp kind: `0` kernel-software, `1` application-fallback |
| *n* | The name, UTF-8, the venue's own label for the connection |

**The magic is eight bytes and not a bare version number** because the two
archive shapes land in one object store under one shipper. A reader handed the
wrong shape has to say so and name the object, rather than read a pcapng Section
Header Block as a record length.

**The format version is in the object and not only in the name.** A name is a
shipper's to change; the bytes are not. A reader that knows only version 1
refuses a version 2 object by number, and says both numbers, rather than
misreading its records.

**The connections are declared up front because they already are.** A
`ConnectionId` in the adapter boundary is a `&'static str` precisely so that a
transport's connections can be named at startup and pre-created as metric
labels. A table in the header therefore costs nothing and spells each name once
rather than once per message.

**The receive-stamp kind is per connection and not per message.** A transport
stamps every payload the same way — the kernel does it, or the transport does —
which is what `Payload` states in refusing to carry the distinction per payload.
Repeating it per record would be a third copy of that taxonomy and a place for
two of them to disagree inside one object.

The byte and the manifest's token are `dz_recorder_core::RecvTsKindLabel`, which
is the same type the `recv_ts_kind` column holds on the publisher side and is
derived from `dz_recorder_core::RecvTsKind`. One taxonomy of receive stamps, and
one place it is spelled: a second enumeration with the same variants agrees with
the first until one side is renamed, and a query filtering on the token across
the two archives then returns the rows of one of them and says nothing.

### A record

Repeated, one per upstream message, in the order the transport yielded them.

| Width | Field |
|---|---|
| 2 | Connection index (`u16`) into the header's table |
| 8 | Receive stamp, nanoseconds since the Unix epoch (`u64`) |
| 4 | Message length in bytes (`u32`), at most 8,388,608 |
| *n* | The message, verbatim |

**Order is receive order, and the writer is where that is fixed.** An adapter
keeps a book, so an object replayed out of order re-derives a different book and
every row that comes out of it is a statement about a market that never
happened.

**Nothing normalises the bytes.** A message the adapter refuses is evidence, and
repairing it destroys the evidence.

**A zero-length message is a message.** It is the case a length-delimited format
is most likely to lose: a reader that read a zero length as *no more records*
would end the object at the first empty keep-alive a venue sends, and report
every message after it as never having arrived.

**The length is bounded at 8 MiB.** The prefix is read out of a file that may be
damaged, and an unbounded length is an allocation that a half-written segment
gets to choose. Eight mebibytes is above the largest thing a venue's own
transport delivers in one piece — a full book response over a polled transport
is the big case, and it is measured in hundreds of kilobytes.

### The end of an object

An object ends where a record ends. Anything else is a refusal that names the
object, and never a short read.

**This is the one property in the format worth stating twice.** A reader that
stopped quietly at a half-written record would hand a derivation fewer messages
than were written, with nothing anywhere saying so — and the rows that came out
would describe a venue that went quiet at the instant the segment was cut. A
silent stop and a genuine outage produce the same rows, so the reader has to be
the thing that can tell them apart.

The refusal states which of the two places the object was cut in — a record
header or a message body — how many whole messages had been read, and how many
bytes were wanted against how many were there. A reader that has refused stays
refused: a caller looping until it gets a clean end must not be able to walk
past a truncation by asking again, because the next call would be reading a
record header out of the middle of a message body.

## The object that lands

| | |
|---|---|
| Extension | `dzus`, or `dzus.zst` when compressed |
| Key | `feed=<feed>/env=<env>/site=<site>/recorder=<recorder>/date=<YYYY-MM-DD>/hour=<HH>/<start>-<end>-<segment>.dzus[.zst]` |
| Digest | `sha256` of the bytes that land, compression included |
| Manifest | `<object>.manifest.json` beside it |

**`dzus` and not `pcapng`.** The extension is the only cheap discriminator an
object store has, and a shipper that put one shape under the other's extension
would hand a pcapng reader bytes it cannot refuse gracefully. A test asserts the
two are different rather than leaving it to a reader to notice.

**The key is the datagram archive's own layout**, produced by the same function,
so a cross-site comparison is a partition prune on both shapes and a shipper has
to learn one scheme.

**The digest and the key are produced at publication and are not derived at read
time.** They are what a re-derivation is idempotent on. A reader that hashed the
bytes it had just decompressed would be answering a different question from the
one the manifest answers — and a truncated object would still have a digest.

The manifest carries, in addition to the key, the digest and the byte count: the
format version, the site, the recorder, the environment, the feed, the
observation, the connections, the segment sequence, the window's first and last
receive stamps, and the message count. Every one of those is computed from state
the writer already held while the segment was open. Nothing re-reads the object:
a manifest produced by reading the object back would be a second decode of the
same bytes, and the only thing it could add is a second opportunity to disagree.

## What the format does not carry

Named, so that nobody adds one of them by inferring it from an absence.

- **No `Channel ID`, `Sequence Number`, `Reset Count` or `segment_seq` per
  message.** Those belong to a channel instance, and a venue's upstream message
  is not on one. The publisher's `Reset Count` span is an era, and the venue side
  has none.
- **No drop count.** `epb_dropcount` is the quantity a capture handle lost, and
  charged to the handle rather than to a port role. A venue transport's loss is
  its session's, measured by the venue's own resend mechanism, and the two must
  not land in one field.
- **No link headers, no destination group, no port role.** There is no link
  layer here, no group and no port.
- **No venue timestamp.** It is a field inside the bytes and it reaches a row
  through the event the adapter produces. An archive that carried it separately
  would let a derivation read it from the wrong place.
- **No instrument identity.** The bytes are what the venue sent; resolving them
  to an instrument is the adapter's, and it happens in the derivation.

## Reading one

```rust
use dz_recorder_archive::upstream::UpstreamObjectReader;

let mut reader = UpstreamObjectReader::open(object_key, bytes)?;
while let Some(message) = reader.next_message()? {
    // message.connection, message.recv_ts_kind, message.recv_ts_ns, message.bytes
}
```

`dz-recorder-venue` is what drives a venue's own `Adapter` over that and
produces rows. Nothing in this repository links a venue: the adapter arrives as
an argument, from the venue's own binary.

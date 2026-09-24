# The shipper contract at `completed_dir`

The recorder does not upload. It writes objects into one directory and stops
there, and `completed_dir` is the whole interface between it and whatever moves
them into object storage.

That split is deliberate and it is what this document exists to hold. Moving
immutable hashed files is a solved problem, and a solved problem stays solved
only if nobody has to teach it a partitioning scheme, a credential rotation or a
retry policy that the process holding the capture ring also has to survive. A
shipper that obeys what is below can be a shell script, a sidecar, or a vendor
agent, and none of them needs to know anything about feeds, channels or pcapng.

This states what the recorder guarantees and what it requires in return. No
shipper in this repository implements it.

---

## What lands, and under what names

Two files per segment, both in `completed_dir`, both named from the same three
numbers. There are two archive shapes and a shipper handles both, because they
land in one object store under one shipper:

```
the multicast archive, the datagrams as captured
  <start_ns>-<end_ns>-<segment_seq>.pcapng.zst
  <start_ns>-<end_ns>-<segment_seq>.manifest.json

the upstream archive, a venue's own payloads
  <start_ns>-<end_ns>-<segment_seq>.dzus.zst
  <start_ns>-<end_ns>-<segment_seq>.dzus.zst.manifest.json
```

`start_ns` and `end_ns` are receive timestamps — the window the recorder can
vouch for — and `segment_seq` restarts at 0 on every recorder run. The
compression suffix is `.zst` by default and absent under no compression; it is
the reader's signal for whether to decode, so it is part of the name rather than
a configuration the reader has to be told about.

**The two extensions differ on purpose.** `pcapng` and `dzus` are the only cheap
discriminator an object store has, and a shipper that put one shape under the
other's extension would hand a pcapng reader bytes it cannot refuse gracefully.
Never rewrite an extension. `UPSTREAM-OBJECT-FORMAT.md` holds the `dzus` shape in
full.

**Note where the manifest suffix goes.** The multicast manifest *replaces* the
object's extension; the upstream manifest is *appended* to the whole object name.
A shipper deriving one name from the other must not assume one rule.

**Neither name is an object key.** Two recorders at two sites rotate segment 5
in the same nanosecond and produce the same file name for different bytes. The
key is in the manifest.

## Publication is an atomic rename, and the object is the signal

Each object is assembled under a hidden temporary name, hashed, and moved to its
final name with a single `rename`. Where the temporary lives differs by shape:

| Shape | Assembled as | In |
|---|---|---|
| multicast (`pcapng`) | `.<name>.part` | the staging directory |
| upstream (`dzus`) | `.<name>.tmp` | `completed_dir` itself |

So a shipper listing `completed_dir` can see an upstream object's temporary,
and never a multicast one's. Either way **a partial object is never visible
under its final name**, and a shipper may begin uploading the instant it sees
one.

Both shapes order their two renames the same way, and the order matters to a
shipper:

1. the manifest is moved into `completed_dir`,
2. then the object.

So **watch for the object and read the manifest beside it — never the other way
round.** A shipper triggering on manifests will see one before its object
exists. If the object's move then fails, the recorder deletes the manifest it
had already placed, because a manifest with no object is an index row pointing
at nothing.

## Where it goes: the manifest says

The manifest's `object_key` is the Hive-partitioned key the object is to land
under, relative to a bucket:

```
feed=<feed>/env=<env>/site=<site>/recorder=<recorder>/date=YYYY-MM-DD/hour=HH/<file name>
```

`date` and `hour` are UTC, derived from `start_ns`, and **both shapes use the
same function** — so a shipper learns one scheme. The recorder decides the layout
and states it; the shipper prefixes a bucket and uploads, and nothing else
derives a path. Hive partitioning is what lets the object store be queried as a
table with no separate catalogue, and `site` and `recorder` are in the key so
that a cross-site comparison is a partition prune rather than a full scan.

## Ship both files

The manifest is not a convenience beside the object. It is the row that
`recorder.segment_coverage` is loaded from, and loading it is what answers a
coverage question without opening a single object — which is what makes a
**missing** object visible at all. A hole in `segment_seq` for a recorder run is
a hole in the archive, and without the manifests a recorder that was down for an
hour cannot be told from a feed that was quiet for an hour.

An object shipped without its manifest is an object nothing will ever load.

## Integrity, and the key reprocessing turns on

The manifest's `sha256` and `byte_count` cover **the object as it lands** — the
compressed bytes, not the pcapng inside — because those are the bytes a consumer
fetches. Verify after upload.

Reprocessing in the analysis tier is idempotent on `(object key, sha256)`. A
shipper that re-uploads the same object is harmless. A shipper that rewrites,
recompresses or repackages an object breaks that pair and is not a shipper.

**Objects are immutable.** Never append, never re-hash, never renumber.

## Delete after upload, and what happens if you do not

Deleting an object and its manifest once both are safely in object storage is
expected, and the recorder tolerates a file the shipper has already taken.

If `completed_dir` is not drained, the multicast recorder does **not** block. A
watermark covers everything on the disk it fills — the staging directory and
`completed_dir` together — and when it is exceeded the oldest multicast objects
are deleted, with their manifests, and counted in
`dz_recorder_segments_evicted_total`.

**The watermark covers the multicast shape only.** It classifies files by the
`pcapng` and `pcapng.zst` names, so a `dzus` object is to it a file it did not
write: not counted against the budget, and never evicted. Nothing else bounds
them either. For the upstream shape, the shipper draining `completed_dir` is
the only thing standing between a storage outage and a full disk.

This is the design's one deliberate loss, and the reason is worth stating to
whoever operates the shipper: a writer that blocked on a full disk would stall
the drain thread, overflow the receive queue, and lose live data. An object
storage outage, an expired credential or a slow disk would become a feed-loss
incident and a run of false publisher-loss findings in every archive written
during it. Deleting the oldest object loses history instead, which is bounded,
counted and alertable.

**So an undrained `completed_dir` is a retention alert, not an outage** — and it
is silent unless somebody watches that counter.

## What a shipper may leave in the directory

A shipper may keep its own state file in `completed_dir`. The recorder does not
delete files it did not write, and does not count them against the watermark: a
stray file large enough to exceed the budget on its own would otherwise make
every eviction pass delete every object without the total ever falling, losing
the archive to a file eviction cannot reach.

**Every name beginning with a dot belongs to the recorder.** An object under
assembly is `.<name>.part` or `.<name>.tmp` depending on which shape is being
written, and both are hidden precisely so that a shipper matching object names
never sees a partial object. A segment whose run ended before it rotated is
`segment-<seq>.recovered-<ns>.pcapng`, adopted by the next run. Leave all of
them alone.

What is yours to take, once uploaded, is a file named
`<digits>-<digits>-<digits>.` followed by `pcapng`, `dzus`, either of those with
`.zst`, or the matching `manifest.json`.

---

## The contract in one list

The recorder guarantees:

- an object under its final name is complete and hashed;
- the manifest for an object is present before the object is;
- the manifest states the object key, the digest and the byte count;
- a multicast object nothing removes will eventually be evicted and counted,
  and the record path will not stall waiting for anybody. An upstream object
  is not evicted, and nothing bounds it but the shipper.

The shipper must:

- trigger on the object, not the manifest;
- upload both files, to the key the manifest states;
- verify the digest;
- never modify an object;
- delete both once they are durable;
- leave dotfiles and `.recovered-` files alone;
- and be watched, because the recorder's response to a shipper that stopped is
  to delete multicast history quietly and count it, and to let upstream objects
  fill the disk.

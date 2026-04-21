# photovault v1.5 Roadmap

## Purpose

This document defines the **photovault v1.5 execution roadmap**.

It is used to:
- guide implementation sequencing after v1
- align PM / TPM / EM decisions for the next server-heavy phase
- constrain scope creep while M5 client networking work is paused for hardware
- steer Codex and other agents toward correct priorities

This roadmap is **authoritative for v1.5 scope**.  
Anything not listed here is either **explicitly deferred** or **out of scope**.

For the v1 delivery roadmap, see:
- `docs/photovault_v1_roadmap.md`

---

## v1.5 Product Definition

**photovault v1.5 delivers:**

> A server-centered follow-on release that turns uploaded and indexed files into an inspectable media system, adds explicit client identity and approval, tracks live client presence, and introduces metadata extraction plus still-image previews for an operational media library.

### Core guarantees
- Filesystem-backed server media storage from v1 remains the source of file content
- PostgreSQL becomes the source of truth for media catalog, client identity, approval state, heartbeat state, and extracted metadata
- SHA256 remains the source of truth for deduplication and file integrity
- Metadata extraction and preview generation are visible, deterministic, and retry-safe
- Client approval is explicit and server-controlled
- Running-client and running-job visibility come from client heartbeats, not server polling

---

## Milestone Overview

| Milestone | Name | Goal |
|---------|-----|-----|
| V1.5-A | Server Catalog Foundation | Persist media catalog metadata tied to server-side filesystem storage |
| V1.5-B | Client Identity & Approval | Add explicit client enrollment, approval, and long-lived trust |
| V1.5-C | Client Presence & Running Jobs | Make clients and current workload visible on the server via heartbeat snapshots |
| V1.5-D | Preview Pipeline & Media Library | Add still-image previews and a practical server-side library UI |
| V1.5-E | Hardening, Backfill, and Acceptance | Backfill, operationalize, and validate the complete v1.5 system |

Milestones are **sequential**.  
Later milestones must not weaken guarantees from v1 or earlier v1.5 milestones.

---

## Milestone V1.5-A – Server Catalog Foundation

### Goal
Turn server-side stored files into a queryable media catalog while keeping filesystem-backed storage as the file-content truth.

### Epics
- PostgreSQL-backed media asset registry tied to stored filesystem paths
- EXIF and media metadata extraction on upload finalize
- EXIF and media metadata extraction during explicit storage indexing
- Normalized metadata persistence for searchable library use
- Provenance linkage between stored file, job context, and client source

### Must-have outcomes
- Every uploaded or indexed media file can have a corresponding media-catalog record
- Extracted metadata is persisted independently of file-content storage
- Extraction failures are recorded and visible without invalidating the stored file
- Catalog records can distinguish uploaded content from indexed pre-existing storage
- No editing or organization workflows are required yet

### Explicit non-goals
- End-user album management
- Free-form tagging
- Face recognition
- Client-side metadata editing

---

## Milestone V1.5-B – Client Identity & Approval

### Goal
Introduce explicit client identity and server-controlled approval before a client becomes fully trusted.

### Epics
- Bootstrap-token client enrollment
- Pending / approved / revoked client lifecycle
- Per-client long-lived auth token issuance after approval
- Server UI for client approval and revocation
- Server-side auditability of enrollment and approval changes

### Must-have outcomes
- A new client can identify itself to the server using a shared bootstrap token
- First contact creates a pending client record on the server
- Pending clients do not receive normal upload privileges
- Approved clients receive a per-client long-lived auth token or equivalent persisted credential
- Revoked clients lose normal operational access until re-approved or re-enrolled

### Explicit non-goals
- mTLS-only trust model
- Enterprise identity integration
- Multi-user role-based admin system

---

## Milestone V1.5-C – Client Presence & Running Jobs

### Goal
Make the server aware of which clients exist, whether they are alive, and what they are doing right now.

### Epics
- Periodic client heartbeat endpoint
- Latest heartbeat snapshot persistence in PostgreSQL
- Client list overview in server UI
- Running-job and current-state overview derived from heartbeat data
- Last-seen and recent-error visibility

### Must-have outcomes
- Each approved client can periodically report its presence and current status to the server
- Heartbeats carry client identity, daemon state, active job summary, retry/backoff summary, and recent errors
- Server UI can show a list of clients with last-seen status
- Server UI can show which clients currently have running or blocked work
- Running-job visibility comes from latest successful heartbeat snapshots, not inferred upload traffic

### Explicit non-goals
- Server polling of client endpoints
- Real-time streaming transport requirements
- Full remote control of client operations from the server UI

---

## Milestone V1.5-D – Preview Pipeline & Media Library

### Goal
Provide practical still-image previews and a useful operator/admin media library on the server.

### Epics
- Filesystem sidecar preview cache
- Still-image preview generation for JPEG, HEIC, and RAW
- RAW preview extraction using embedded JPEG first, fallback external converter second
- Read-only media library browse/search/filter/detail UI
- Lightweight organization flags: `favorite` and `hidden/archive`
- Bootstrap-based server UI styling using Bootstrap and optional Font Awesome Free

### Must-have outcomes
- Preview generation is cached and deterministic
- Preview generation is best-effort and visible, not hidden
- RAW preview generation prefers embedded previews before invoking slower fallback conversion
- The library can filter at minimum by date, client, job, media type, favorite, and hidden/archive state
- Media detail pages can show metadata, provenance, and preview availability
- Videos remain cataloged even if poster generation is not yet implemented

### Explicit non-goals
- Consumer-facing DAM experience
- Albums or collections
- Free-form tags
- Video poster generation in the first v1.5 slice
- Rich collaborative organization workflows

---

## Milestone V1.5-E – Hardening, Backfill, and Acceptance

### Goal
Backfill existing storage, harden operational behavior, and validate the full v1.5 server stack end to end.

### Epics
- Metadata backfill for previously indexed or stored content
- Preview backfill for existing still media
- Operational tooling for failed extraction and preview generation
- Acceptance validation for mixed uploaded and indexed libraries
- Acceptance validation for approved, pending, and revoked clients

### Must-have outcomes
- Existing indexed storage can be brought forward into the metadata and preview model without re-uploading files
- Failed extraction and preview attempts are visible and retryable
- Mixed uploaded plus indexed libraries behave consistently in the catalog and UI
- Client identity, approval, heartbeat, and running-job views are credible in daily operations
- v1.5 can be accepted without depending on M5 networking completion

### Explicit non-goals
- Full automatic repair/orchestration of all failed assets
- Background distributed processing platforms
- Large-scale search infrastructure beyond the project’s PostgreSQL-based scope

---

## Public Interfaces Planned by v1.5

### Client auth / enrollment
- Client enrollment endpoint using a bootstrap token
- Admin approve / revoke endpoints or equivalent server UI actions
- Per-client auth token issuance, storage, and validation

### Client presence
- Heartbeat endpoint carrying:
  - client identity
  - daemon state
  - active job summary
  - file counters
  - retry/backoff summary
  - recent errors
- Admin endpoints and UI data for client list and running-job overview

### Media catalog
- PostgreSQL media-asset metadata model linked to stored files
- EXIF extraction result model linked to media assets
- Preview-cache path model linked to media assets
- Library list and detail endpoints for server UI

### Library organization
- `favorite` flag
- `hidden/archive` flag
- No albums, collections, free-form tagging, or multi-user workflows in v1.5

---

## Operational Behavior

The following implementation expectations are part of the roadmap and should be treated as policy:

- EXIF and media metadata extraction run for both uploaded files and explicitly indexed existing storage
- Extraction failures are recorded and visible; they do not invalidate the stored file
- Preview generation is cached and deterministic
- Preview generation is best-effort and visible, not hidden
- Pending or revoked clients do not get normal upload privileges
- Approved clients remain identifiable independently of the shared bootstrap token
- Running-jobs view is based on latest successful heartbeat snapshot, not inferred upload traffic alone
- Filesystem-backed media storage remains the source of file content; PostgreSQL is the source of truth for catalog, client, approval, heartbeat, and metadata state

---

## Explicitly Deferred (Not v1.5)

- Replacing filesystem-backed server storage with object storage
- Albums, collections, and free-form tagging
- Face recognition and semantic search
- Consumer-facing gallery experience
- Video poster generation in the first preview slice
- Server polling of client endpoints
- Enterprise identity and access management
- Full remote-control orchestration of client state machines

---

## Acceptance Criteria

photovault v1.5 is complete only if:
- upload of new media results in metadata extraction, catalog entry, and preview availability where applicable
- explicit indexing of existing storage results in the same catalog behavior without re-uploading content
- a new client can enroll with a bootstrap token and remain pending until approved by the server
- an approved client can authenticate, heartbeat, and perform normal operations
- a revoked client is rejected for further normal operations
- the server UI can show multiple clients with last-seen status and currently running job summaries
- the media library supports practical browse/filter/detail workflows for stored media
- favorite and hidden/archive organization flags affect library filtering correctly
- RAW previews use embedded-preview extraction first, falling back to an external converter only when needed
- video assets remain cataloged even when poster generation is deferred

### Acceptance-oriented scenarios
- Upload a new file and confirm metadata extraction, catalog entry, and preview availability
- Index an existing storage tree and confirm the same metadata/catalog behavior without re-upload
- Enroll a new client with bootstrap token and verify server-side pending approval before normal access
- Approve a client and confirm ongoing authenticated heartbeats and upload access
- Revoke a client and confirm server-side rejection of further normal operations
- Show multiple clients in the server UI with last-seen status and currently running job summaries
- Browse the media library by date, client, and job and inspect EXIF plus provenance on a detail page
- Mark media as favorite and hidden/archive and confirm library filtering reflects that state
- Confirm RAW files get previews via embedded-preview extraction first, with fallback converter only when needed
- Confirm video assets remain cataloged even when poster generation is not yet implemented

---

## Guidance for Codex / Agents

- Keep v1.5 server-heavy while M5 client networking waits on hardware
- Preserve filesystem-backed media storage as the file-content truth
- Keep PostgreSQL as the source of truth for catalog, client identity, approval, heartbeat, and metadata
- Prefer explicit, auditable background work over hidden heuristics
- Keep the first media library operator/admin-oriented, not consumer-facing
- Prefer Bootstrap-based SSR UI patterns over SPA complexity
- Surface preview/extraction failure states explicitly

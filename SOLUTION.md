# SOLUTION.md

## What was broken, and why

**1. Duplicate call records / drifting call-counts**
`EventExists` then `InsertEvent` was a check-then-act across two separate round trips, with no unique constraint backing it up (`idx_events_event_id` was a plain index, not `UNIQUE`). Under concurrent redelivery of the same `event_id`, two requests could both pass the existence check before either had inserted, so both proceeded to insert, upsert the call, and increment `account_stats`. A test that fires 50 concurrent requests with the same `event_id` reproduced up to 47 duplicate rows in `events` for a single event in one run, and the account's `call_count` was inflated to match.

**2. Recordings never marked processed, nothing in the logs**
`processRecording` ran in a goroutine seeded with `r.Context()` — the request's context. `net/http` cancels that context the moment the handler returns, which happens right after the goroutine is launched, well before the 50ms simulated recording fetch (`recordingWork`) completes. `MarkRecordingProcessed` then failed with `context canceled`, and the error was silently dropped by an empty `// TODO: handle`.

**3. In-flight work vanishing on deploy**
Even after fixing the context bug above, nothing tracked the detached `processRecording` goroutines. `srv.Shutdown()` only waits for active HTTP requests, and the request had already returned before the goroutine finished — so on `SIGTERM`, whatever recording was mid-flight was simply abandoned.

**4. Stats cache data race**
`Cache.Record` mutated the underlying map without taking `mu.Lock()`, while `Cache.Get` correctly took `mu.RLock()`. A 100-goroutine concurrent test caught this directly with `go test -race`, and lost 14 of 100 increments to the race before the fix — a second, independent source of the "call-counts are drifting" symptom, on top of the DB-level race.

**5. Stale stats after restart**
The in-memory stats cache (`stats.NewCache()`) starts empty on every process boot and was never re-synced from the durable `account_stats` table. `GET /accounts/{id}/stats` only reads the cache, so a restart made account totals read as zero even though Postgres had the correct numbers the whole time — no data was lost, but it was unreadable through that endpoint until new events repopulated the cache.

## Why this deduplication strategy over the alternatives

I used a Postgres `UNIQUE` constraint on `events.event_id`, with `InsertEvent` rewritten to `INSERT ... ON CONFLICT (event_id) DO NOTHING`. `Ingest` now checks whether that insert actually added a row (via `RowsAffected()`) and only runs `UpsertCall`, `IncrementAccountStats`, the cache update, and the recording kickoff if it did. That makes the insert itself the single atomic dedup gate — there's no window between "check" and "act" for two concurrent requests to both slip through, because it's one statement instead of two.

I considered a Redis `SETNX event_id` pre-check instead. I didn't go with it because Redis isn't the durable source of truth here — a dedup key that only lives in Redis reintroduces a race between "Redis says this is new" and "Postgres actually committed it," and any Redis data loss (eviction under memory pressure, a restart without persistence enabled) would silently reopen the double-counting bug it was supposed to close. Postgres already has to be the transactional home for `events`, `calls`, and `account_stats`, so putting the idempotency guarantee anywhere else just adds a second source of truth that has to be kept in sync with the first. The unique constraint gets the same guarantee for free, backed by the database that's already authoritative.

## What I'd change at 10,000 webhooks/second

The current design is a single write per event against a table where every event for the same account contends on the same `account_stats` row (`UPDATE ... SET call_count = call_count + 1`). At 10k/s that row becomes a lock-contention hotspot for any account with meaningful traffic, and the check-and-insert-then-three-more-writes shape of `Ingest` means each webhook costs several round trips to Postgres.

I'd move `IncrementAccountStats` off the synchronous request path — buffer increments (e.g. via Redis `INCR`, which is exactly the kind of thing it's suited for) and flush/reconcile into Postgres periodically instead of writing on every single request. I'd also replace the per-request `go func()` for recording processing with a bounded worker pool or a real queue (Redis Streams, or a jobs table) instead of one goroutine per webhook — at that volume, an unbounded goroutine-per-request model is itself a resource problem, and a real queue also makes the graceful-shutdown story better than the `WaitGroup` drain I built here: work just resumes after a restart instead of needing to finish before shutdown completes.

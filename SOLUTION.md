# SOLUTION.md

## What was broken

there were these issues that i identified and fixed

1. duplicate call records which caused drifting call counts
2. recordings that never marked processed
3. in-flight work vanishing on deployment
4. stats cache had a data race, also contributed to drifting counts
5. stats reset to zero after a restart even though postgres had the right numbers

**1. dup records:** there was no unique constraint on event_id, it was check-then-execute across 2 separate round trips (EventExists, then InsertEvent). concurrent redelivers of the same event_id could both pass the check before either inserted. the test i ran with 50 goroutines gave 47 duplicate records for one event_id, and account_stats got incremented once per duplicate.

**2. recordings never processed:** ctx was cancelled by net/http when the handler returns, before the 50ms simulated recording work finished. the goroutine in Ingest was using r.Context(), which dies right after the request completes. error from MarkRecordingProcessed was getting swallowed by an empty `// TODO: handle` so nothing showed up in logs either.

**3. in-flight work vanishing on deploy:** even after fixing #2's ctx, nothing was tracking the background goroutines. srv.Shutdown() only waits on live http requests, and the request already returned before the goroutine finished, so SIGTERM just abandoned whatever was mid-flight.

**4. cache race:** Cache.Record wrote to the map with no lock, Get used RLock but Record used nothing. caught it with go test -race, a 100 goroutine test lost 14 increments before the fix.

**5. stale cache on restart:** cache is just an in-memory map, starts empty every boot, nothing re-syncs it from account_stats. GET /accounts/{id}/stats reads cache only so it read zero after a restart even though the durable numbers were fine.

## dedup strategy

went with a postgres unique constraint on events.event_id + INSERT ... ON CONFLICT (event_id) DO NOTHING. Ingest checks RowsAffected to know if the insert actually happened, and only runs UpsertCall / IncrementAccountStats / cache update / recording kickoff if it did. one atomic statement instead of check-then-act, so no window for two concurrent requests to both get through.

considered redis SETNX instead. didn't go with it because redis isn't the durable source of truth here, a dedup key living only in redis reintroduces the same race between "redis says its new" and "postgres actually committed", and if redis loses that key (eviction, restart without persistence) the double counting bug just comes back. postgres already has to own events/calls/account_stats so putting the guarantee somewhere else means keeping two sources of truth in sync for no real benefit.

## at 10k webhooks/sec

account_stats becomes a hotspot, every event for the same account hits the same row (call_count = call_count + 1), that serializes under load. and each webhook currently costs like 4 round trips to postgres synchronously.

would move IncrementAccountStats off the request path, buffer it (redis INCR probably) and reconcile into postgres periodically instead of a write per webhook. would also swap the per-request go func() for recording processing with a bounded worker pool or a real queue (redis streams / a jobs table) instead of one goroutine per request, unbounded goroutines don't scale and a real queue also makes shutdown simpler than the WaitGroup drain i built, work just resumes after restart instead of needing to finish before shutdown.

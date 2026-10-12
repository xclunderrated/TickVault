# Delivery options evaluated on 2026-10-12

| Option | Decision | Evidence required to activate |
| --- | --- | --- |
| Hourly BI5 feed | Supported with shared admission and an explicit access canary | A nonempty real payload; any stricter confirmed vendor allowance |
| Local archive | Implemented: validated hourly receipts, offline reads and streaming exports | Requested range must have actual downloaded or explicit no-data coverage |
| Daily S3 export | A separate future adapter; no transfers initiated | User AWS credentials/billing authorization, bounded real sample and verified coverage/units |
| Stable user-supplied proxy | Supported within the same provider budget | Provider permission for the route; credentials supplied at runtime |
| Route change after 429 | No automatic quota reset | A verified independent allowance would be needed |
| Jetta JSON endpoint | Not selected as an hourly BI5 substitution | Current contract, pagination, units, exact bounds and bounded real sample |
| Independent data provider | No source silently substituted | Explicit source selection and bid/ask, timestamp, coverage and unit verification |

[Dukascopy's official export guide](https://www.dukascopy.com/wiki/en/development/data-export/)
documents Requester Pays S3 access in eu-west-1. It describes daily LZMA BI5
objects at `SYMBOL/YEAR/MONTH/DAY_ticks.bi5`, with zero-based months and 20-byte
records whose timestamp is milliseconds since UTC day start. Its timebase differs
from the hourly decoder. Scaling must be verified per instrument. AWS costs and
actual object coverage need confirmation before a paid adapter is activated.

A daily object could replace up to 24 hourly fetches for that day's covered
instrument; this is a format-based inference, not measured throughput or an
unthrottled-service promise. An adapter should preserve source provenance and
publish hourly receipts only after day-level validation and lossless splitting.

The hourly access canary received 429 with no Date, Retry-After, Server or Via
header. Those diagnostics do not establish which network component produced
the response. The selected route has no validated live payload, so increasing
traffic or treating another IP as a fresh allowance is unsupported.

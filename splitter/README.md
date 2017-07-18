splitter
===

This splitter allows ingesting logs from a CWLogs subscription.

Splitter's expected input is batched logs from a CloudWatchLogs subscription to a Kinesis Stream.
The CWLogs subscription has a special format which bundles several logs into a single record.
The splitter takes this record and splits it into multiple logs.
These logs are also modified to mimic the RSyslog format we expect from our other logs.
This allows them to be decoded normally by the rest of the pipeline.

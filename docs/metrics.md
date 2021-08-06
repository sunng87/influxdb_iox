# Metrics in IOx

IOx output metrics to Jaeger for distributed request correlation.

Here are useful metrics

### Requests to IOx Server including Routers and Query Servers
| Metric name |  Code Name | Description |
| --- | --- | --- | 
| http_requests_total | http_requests | Total number of HTTP requests |
| gRPC_requests_total | requests | Total number of gROC requests |


### Line Protocol Data ingested into Routers

| Metric name |  Code Name | Description |
| --- | --- | --- | 
| ingest_points_total | ingest_lines_total | Total number of lines ingested |
| ingest_fields_total | ingest_fields_total | Total number of fields (columns) ingested |
| ingest_points_bytes_total | ingest_points_bytes_total | Total number of bytes ingested |
| ingest_entries_bytes_total |  ingest_entries_bytes_total | Total number of entry bytes ingested (Not sure what this means |
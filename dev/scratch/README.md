# dev/scratch/

Exploratory query and discovery scripts written during development of the collector modules.

These are **not production code** — they were used to explore RSC GraphQL schema, test CDM API endpoints, and prototype data structures before the logic was formalized into the `collectors/` modules.

Kept here for reference in case specific query patterns are needed again.

| Script | Purpose |
|--------|---------|
| `debug_queries.py` | Ad-hoc RSC GraphQL query testing |
| `discover_schema*.py` | RSC schema introspection |
| `discover_workloads.py` | Early workload discovery prototypes |
| `discover_linux_hosts*.py` | Linux host enumeration iterations |
| `discover_hosts_final.py` | Final host enumeration before collector refactor |
| `discover_filesets*.py` | Fileset and fileset host discovery |
| `discover_db_hosts.py` | Database host discovery |
| `discover_exchange*.py` | Exchange workload discovery |
| `discover_mysql*.py` | MySQL workload discovery |
| `discover_postgres*.py` | PostgreSQL workload discovery |
| `discover_mv.py` | Managed volume discovery |
| `discover_jobs.py` | Job history discovery |

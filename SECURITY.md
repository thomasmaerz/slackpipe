# Security

Report vulnerabilities privately through GitHub Security Advisories. Do not
open public issues containing Slack tokens, cookies, exported messages, archive
paths, or deployment credentials.

Slackpipe treats workspace credential files, raw Slackdump archives, canonical
DuckDB files, attachment trees, and Dagster/PostgreSQL credentials as secrets.
Keep them outside version control, restrict filesystem permissions, and expose
Dagster and Pushgateway only through a trusted network or authenticated proxy.

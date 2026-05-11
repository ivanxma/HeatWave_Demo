# HeatWave_Demo

This application now uses Flask instead of Streamlit while preserving the same core logic:

- login with a saved MySQL connection profile
- optionally connect through an SSH tunnel defined on the saved profile
- maintain `nlsql.configdb`
- run HeatWave `sys.NL_SQL`
- run HeatWave visual prompts with `sys.ML_GENERATE`
- build and query a HeatWave GenAI knowledge base from URL content
- configure and run an AskME-style GenAI knowledge base backed by `askme.config`
- run HeatWave AutoML demo actions on the Iris dataset
- compare InnoDB and RAPID execution on `airportdb`

## Install

Install `git` first, depending on your operating system:

### Ubuntu

```bash
sudo apt update
sudo apt install -y git python3 python3-pip python3-venv python3-full
```

### Oracle Linux 8

```bash
sudo dnf install -y git python3 python3-pip python3-setuptools python3-wheel
```

### Oracle Linux 9

```bash
sudo dnf install -y git python3 python3-pip python3-setuptools python3-pip-wheel
```

Clone the repository and enter the project directory:

```bash
git clone https://github.com/ivanxma/HeatWave_Demo.git
cd HeatWave_Demo
```

Run the setup script to install the OS packages, create `.venv`, install MySQL Shell Innovation, persist runtime defaults, and optionally configure services:

```bash
chmod +x setup.sh start_http.sh start_https.sh
./setup.sh ol9 both --http-port 80 --https-port 443
```

The setup script:

- supports `ol8`, `ol9`, `ubuntu`, and `macos`
- supports deploy modes `http`, `https`, `both`, and `none`
- prompts interactively for omitted deploy mode and matching listener ports
- installs `python3`, `pip`, and `openssl` where the platform needs them
- creates `.venv` in the project directory
- installs the Python packages from `requirements.txt` into that virtual environment
- creates `.runtime.env` with saved listener defaults
- creates `heatwave-demo-http.service` and `heatwave-demo-https.service` on Linux when `systemd` is available
- opens only the selected HTTP/HTTPS ports through `firewall-cmd` on Oracle Linux or `ufw` on Ubuntu when available
- falls back to direct startup instructions when a platform service manager is unavailable

Platform-specific MySQL Shell Innovation installers are under:

- `ol8/install_mysql_shell_innovation.sh`
- `ol9/install_mysql_shell_innovation.sh`
- `ubuntu/install_mysql_shell_innovation.sh`
- `macos/install_mysql_shell_innovation.sh`

## Run

If services were created, manage them with `systemctl`:

```bash
sudo systemctl status heatwave-demo-http.service
sudo systemctl status heatwave-demo-https.service
sudo journalctl -u heatwave-demo-https.service -f
```

The launcher generates a self-signed certificate under `tls/` if HTTPS is selected and no certificate/key path is supplied.

If `systemd` is not available, start the app directly:

```bash
/bin/bash ./start_http.sh
/bin/bash ./start_https.sh
```

## OCI Compute Setup

Create an OCI Compute instance and paste the matching initialization script into the instance metadata.

1. In the OCI Console, open `Compute > Instances` and click `Create instance`.
2. Choose the image:
   - Oracle Linux 9, or
   - Ubuntu.
3. Choose the shape, VCN, subnet, SSH key, and public-IP settings required for your environment.
4. Expand `Show advanced options`.
5. Open the `Management` tab.
6. Paste one of the following scripts into `Initialization script`.
7. Create the instance.
8. SSH to the instance as `opc` after boot. The login banner shows whether installation is still running, succeeded, or failed.

Oracle Linux 9 initialization script:

```bash
#!/bin/bash
set -euxo pipefail

dnf install -y curl
curl -fsSL https://raw.githubusercontent.com/ivanxma/HeatWave_Demo/main/oci_compute_init.sh -o /tmp/oci_compute_init.sh
chmod 0755 /tmp/oci_compute_init.sh
OS_FAMILY=ol9 \
APP_REPO=https://github.com/ivanxma/HeatWave_Demo.git \
DEPLOY_MODE=both \
HTTP_PORT=80 \
HTTPS_PORT=443 \
bash /tmp/oci_compute_init.sh
```

Ubuntu initialization script:

```bash
#!/bin/bash
set -euxo pipefail

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y curl
curl -fsSL https://raw.githubusercontent.com/ivanxma/HeatWave_Demo/main/oci_compute_init.sh -o /tmp/oci_compute_init.sh
chmod 0755 /tmp/oci_compute_init.sh
OS_FAMILY=ubuntu \
APP_REPO=https://github.com/ivanxma/HeatWave_Demo.git \
DEPLOY_MODE=both \
HTTP_PORT=80 \
HTTPS_PORT=443 \
bash /tmp/oci_compute_init.sh
```

The init script clones or refreshes the repository under `/home/opc/HeatWave_Demo`, runs `setup.sh`, and records status under `/var/lib/heatwave-demo-init`. The login banner for `opc` prints `Please wait until installation to be completed.` while setup is running, `The service is installed.` with `systemctl status` after success, or a failure message pointing to `/var/log/heatwave-demo-init.log`.

Check the deployed service:

```bash
sudo systemctl status heatwave-demo-http.service
sudo systemctl status heatwave-demo-https.service
sudo journalctl -u heatwave-demo-https.service -f
```

## Admin Auto-Update

After login, use `Admin > Update HeatWave_Demo` to refresh the app from its configured git repository. The updater requires a clean worktree, runs `git fetch --all --prune`, runs `git pull --ff-only`, reruns `setup.sh`, and restarts active `heatwave-demo-http.service` or `heatwave-demo-https.service` services when systemd is present.

For a full service refresh from the web UI, the service user needs passwordless `sudo` for setup and service restart operations. If passwordless `sudo` is unavailable, the updater falls back to `SKIP_PRIVILEGED_SETUP=1`, refreshes the repo and Python environment, and lets systemd recover the service by restarting the current process. Run `./setup.sh` manually later if privileged package, firewall, TLS ownership, or service-unit changes were skipped.

## AirportDB Loader

The repository also includes helper scripts to install MySQL Shell Innovation and load the MySQL `airportdb` sample dump used by the HeatWave quickstart.

OS-specific MySQL Shell installation scripts:

- `ol8/install_mysql_shell_innovation.sh`
- `ol9/install_mysql_shell_innovation.sh`
- `ubuntu/install_mysql_shell_innovation.sh`

The wrapper script [load_airportdb.sh](load_airportdb.sh) does the following:

- detects the Linux distribution
- installs `mysqlsh` with the matching script if `mysqlsh` is not already present
- downloads `airport-db.tar.gz`
- extracts it with `tar xvzf airport-db.tar.gz`
- runs `util.loadDump("airport-db", {threads: 16, deferTableIndexes: "all", ignoreVersion: true})`
- optionally calls `sys.heatwave_load` for schema `airportdb`

Usage:

```bash
chmod +x load_airportdb.sh
./load_airportdb.sh <mysql_user> <mysql_host_or_ip>
```

Optional environment variables:

```bash
LOAD_THREADS=16
HEATWAVE_LOAD=1
AIRPORTDB_ARCHIVE_PATH=/path/to/airport-db.tar.gz
AIRPORTDB_EXTRACT_DIR=/path/to/airport-db
MYSQLSH_BIN=/usr/bin/mysqlsh
```

Notes:

- `mysqlsh` will prompt for the password interactively.
- The script expects the extracted dump directory to be `airport-db` under the project root unless `AIRPORTDB_EXTRACT_DIR` is set.
- `HEATWAVE_LOAD=0 ./load_airportdb.sh <mysql_user> <mysql_host_or_ip>` skips the final HeatWave load call.

## Kubernetes

Deploy to Kubernetes in a dedicated `nlsql` namespace:

1. Create the namespace:

```bash
kubectl create ns nlsql
```

2. Apply the manifest:

```bash
kubectl apply -n nlsql -f k8s/hw-nlsql.yaml
```

3. Check the deployment and service:

```bash
kubectl get deploy,po,svc -n nlsql
```

4. If the pod is still starting or restarting, check the logs:

```bash
kubectl logs -n nlsql deployment/hw-nlsql
```

5. If you update the manifest and need to restart the pod:

```bash
kubectl rollout restart deployment/hw-nlsql -n nlsql
kubectl rollout status deployment/hw-nlsql -n nlsql
```

The Kubernetes manifest creates the `Secret`, `ConfigMap`, `Deployment`, and `LoadBalancer` `Service` in namespace `nlsql`.

## Configure

### OCI IAM Setup

If this application runs on OCI Compute and needs to call OCI services by using instance principal authentication, create a Dynamic Group and the required IAM policies first.

#### Create Dynamic Group `hw-genai-dg`

In the OCI Console, open `Identity & Security > Domains > Default domain > Dynamic groups`, then create a Dynamic Group with:

- Name: `hw-genai-dg`
- Description: `Dynamic group for HeatWave GenAI application instances`
- Matching rule: `ANY {instance.compartment.id = 'the ocid of the compartment', resource.compartment.id = 'the ocid of the compartment'}`

Replace `the ocid of the compartment` with the actual compartment OCID.

#### Create Policies

Create a policy in the tenancy or in the target parent compartment and attach the following statements for compartment `hw-demo-compartment`:

```text
allow dynamic-group hw-genai-dg to read volume-family in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to read instance-family in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to read objectstorage-namespaces in tenancy
allow dynamic-group hw-genai-dg to read buckets in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to manage objects in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to manage object-family in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-chat in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-text-generation in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-text-summarization in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-text-embedding in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-model in compartment hw-demo-compartment
```

If you use different OCI names, replace `hw-genai-dg` and `hw-demo-compartment` with your actual Dynamic Group and compartment names. After creating the Dynamic Group and policies, allow a few minutes for IAM propagation before testing the application.

1. Open the app.
2. Create or select a saved connection profile on the login page.
3. Log in with the database user and password for that profile.
4. Open `Admin > Setup configdb` and choose the schemas NL_SQL should use.
5. Open `Admin > Setup ObjectStorage` to create database `askme`, initialize table `askme.config`, configure OCI authentication, and save bucket settings for AskME object-storage usage.
6. Use `HeatWave > NL_SQL`, `HeatWave > HWVision`, `HeatWave > GenAI`, `HeatWave > Askme GenAI`, `HeatWave > HeatWave ML`, or `HeatWave > HeatWave Performance`.

If MySQL is restarted and the active session is no longer valid, the next authenticated page load clears that dead session and routes back to the login page.

`HeatWave > Askme GenAI` is shown only after all required `askme.config` values are configured. AskME database access uses the current logged-in MySQL connection; it does not use OCI Vault to look up database credentials.

Profiles are stored in `profiles.json`. Only non-secret connection details are stored there. A profile can include SSH tunnel settings: jump host, SSH port, SSH user, and private-key path. Database passwords are entered at login and remain in the current app session only.

### Connection Caching

- MySQL connections are cached per login session and per worker thread.
- Cached connections are validated with `ping(reconnect=False)` before reuse.
- Dead or stale cached connections are purged instead of reused.
- Logout, profile changes, and login reset clear cached MySQL connections.
- SSH-tunneled connections are cached together with their tunnel and closed together when purged.
- LLM model dropdown values from `sys.ML_SUPPORTED_LLMS` are cached per login session. Generation and embedding model lists are loaded once per capability and cleared on logout or login reset.

### Connection Timeout Settings

- `Admin > Connection Profile` and the login page let you store optional timeout values per saved profile.
- Supported values are connector-side `connection_timeout` plus session values `net_read_timeout`, `net_write_timeout`, `max_execution_time`, `wait_timeout`, and `interactive_timeout`.
- Leave `connection_timeout` blank to use the app-level safety default of 5 seconds. Leave the session timeout fields blank to use the MySQL or connector defaults for those settings.
- The top-right header shows the compact summary as `Timeout : connection/read/write/max_execution`.
- Clicking the timeout summary opens a popup that shows the current session timeout values by variable name and lets you change them for the active saved profile.
- Updated profile values are applied to each new MySQL connection opened by the app.

### HTTPS Listener Timeouts

- When the app runs with TLS enabled, it now defers the SSL handshake until after the socket has been handed to a worker thread. This prevents one stalled client from blocking the main listener on port `443`.
- `APP_SSL_HANDSHAKE_TIMEOUT` controls the per-connection TLS handshake timeout in seconds. The default is `5`.
- `APP_REQUEST_SOCKET_TIMEOUT` controls the post-handshake client socket timeout in seconds. The default is `30`.

## Page Notes

### NL_SQL

- The `Submit` button is disabled while a request is running.
- The pointer changes to a wait cursor until the response returns.
- The page shows output in one TabView: `CALL`, `Generated SQL`, `Referenced Tables`, and one tab per returned result set.
- The displayed call syntax uses `@output` as the output variable, for example `CALL sys.NL_SQL('question', @output, JSON_OBJECT(...));`.

### GenAI

- The page includes `Create KB` and `Search KB` tabs.
- `Create KB` fetches text from a source URL, chunks the content, generates embeddings, and stores the rows in a vector table.
- `Create KB` can either reuse an existing schema or create a new database before loading the vector rows.
- The result panel shows the source URL, chunk count, inserted row count, a stored-sources summary, and a chunk preview table.
- `Search KB` embeds the question, finds the nearest stored chunks, and passes the matched text into HeatWave text generation to produce the answer.
- Search stays disabled until the selected schema contains the configured vector table.

### Askme GenAI

- `Admin > Setup ObjectStorage` creates database `askme` if needed and creates table `askme.config (my_row_id, env_var, env_value, primary key(my_row_id))`.
- The setup page uses a TabView with `OCI Config`, `Bucket Settings`, `Bucket Upload`, and `Pre-Authenticated URLs`.
- The `OCI Config` tab lets you choose whether to reference an existing config file such as `~/.oci/config` or store a local config and uploaded private key under the git-ignored `oci_config/` folder.
- The setup page stores OCI and bucket settings in `askme.config`: `OCI_REGION`, `OCI_CONFIG_FILE`, `OCI_CONFIG_PROFILE`, `OCI_BUCKET_NAME`, `OCI_NAMESPACE`, and `OCI_BUCKET_FOLDER`.
- The `Bucket Upload` tab can populate folders from the bucket, create a folder marker object, retain the populated folder list in the session, and upload files to root or the selected folder.
- The `Pre-Authenticated URLs` tab creates Object Storage pre-authenticated requests with target type `Objects with Prefix`, optional object listing, a folder dropdown populated from the bucket, and a table of existing PARs.
- The AskME page uses the current logged-in MySQL connection for all database operations.
- `HeatWave > Askme GenAI` is enabled only after the AskME setup values are populated.
- The page includes `Find Relevant Docs`, `Free-style Answer`, `Answer Summary`, `Chatbot`, and `Knowledge Base Management` tabs.
- `Knowledge Base Management` uploads selected files into the configured OCI bucket folder and builds the vector table from explicit `oci://bucket@namespace/object_path` file references.
- The `Chatbot` tab keeps the controls grouped on the left and shows tabbed output on the right for `Messages` and `References`.

### HeatWave Performance

- The menu item appears only when schema `airportdb` exists.
- The page uses two tabs: `InnoDB` and `RAPID engine`.
- Opening or switching tabs only loads the SQL text and metadata. It does not execute the SQL.
- Clicking `Execute` runs the current editable SQL, switches the cursor to wait, and shows `Status : Running` while the request is in flight.
- The page forces the session to `autocommit=1` for this workload and displays the current session autocommit value.
- The page shows row counts for `airportdb.booking`, `airportdb.flight`, `airportdb.airline`, and `airportdb.airport_geo`.
- The `EXPLAIN` output is produced only after `Execute` is clicked and JSON plan values are formatted for readability.

### HeatWave ML

- The page includes an `Iris` tab and an `NL2ML` tab.
- The `Iris` tab initializes `ml_data.iris_train`, `ml_data.iris_test`, and `ml_data.iris_validate`.
- `Initialize IrisDB` recreates schema `ml_data`, loads the Iris demo split used by the page, makes `my_row_id` visible after load, and clears `ML_SCHEMA_<user>.MODEL_CATALOG` for `iris_model`.
- Action buttons show the SQL or procedure syntax in the info row before the request is submitted, then append timing after the request finishes.
- `Execute ML_TRAIN` runs `CALL sys.ML_TRAIN('ml_data.iris_train', 'class', JSON_OBJECT('task', 'classification', 'exclude_column_list', JSON_ARRAY('my_row_id')), @model);`.
- `Execute ML_MODEL_LOAD` runs `CALL sys.ML_MODEL_LOAD("iris_model", NULL);`.
- `Execute ML_PREDICT_ROW` uses a fixed sample row and shows the prediction output in form view on the right panel.
- `Execute ML_PREDICT_TABLE` runs against `ml_data.iris_test`, refreshes the left panel with `iris_test`, and shows `ml_data.iris_predictions` on the right.
- `Execute ML_SCORE` runs `CALL sys.ML_SCORE('ml_data.iris_validate', 'class', @iris_model, 'balanced_accuracy', @score, NULL);` and shows `@score` in form view.
- `Execute ML_EXPLAIN_TABLE` runs `CALL sys.ML_EXPLAIN_TABLE('ml_data.iris_test', @iris_model, 'ml_data.iris_explanations', JSON_OBJECT('prediction_explainer', 'permutation_importance'));` and shows `iris_explanations` in form view.
- The `NL2ML` tab uses a left/right layout.
- The left panel lets you choose a supported generation LLM, toggle `Keep history`, enter a prompt question, and click `Generate`.
- `Generate` only constructs the SQL for `SET @nl2ml_options ...` and `CALL sys.NL2ML(..., @output);`. It does not execute the SQL.
- The right panel shows the editable generated SQL, an `Execute` button, timing, and tabbed output.
- After execution, the tab view shows returned result sets plus `@output` and `@nl2ml_options`.
- The `@output` tab formats the JSON `text` field with preserved line breaks.
- The `@nl2ml_options` tab parses array values into tables. For `chat_history`, the table is pivoted into `user message` and `chat_bot_message` columns.

### HeatWave LH/External Table

- The page includes `HeatWave_load` and `Incremental Refresh` tabs.
- `Incremental Refresh` uses a two-panel layout with Lakehouse databases on the left and Lakehouse tables on the right.
- Selecting a table enables `Definition` and `Refresh` actions for the current table only.
- `Definition` opens a popup that shows `SHOW CREATE TABLE` and allows editing `AUTO_REFRESH_SOURCE`.
- `Refresh` generates the incremental refresh SQL first, then `Execute` runs that SQL and shows the returned result sets below.

### DB Admin

- The old `HeatWave Performance Query`, `HeatWave ML Query`, and `HW Table Load Recovery` tabs are consolidated into one `Monitoring` tab.
- The `Monitoring` tab includes 3 buttons to switch between those views.
- The selected monitoring button is highlighted in dark red. The inactive buttons keep the red gradient style.
- The `Monitoring` toolbar includes a `Refresh` button and an `Auto Refresh` dropdown with `2s`, `5s`, `30s`, `60s`, and `none`.
- When an auto-refresh interval is selected, the monitoring view refreshes automatically using the selected interval.
- The `HeatWave ML Query` monitoring view includes a `Current ML running connection only` filter.
- When that filter is enabled, the main query appends `connection_id = (select id from performance_schema.processlist where info like 'SET rapid_ml_operation%')`.
- When the filter is enabled, the page also shows a second detail table for the latest current-running ML query, including the full `QEXEC_TEXT`.

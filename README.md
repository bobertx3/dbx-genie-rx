<!-- markdownlint-disable MD033 -->
# GenieRx: Analyze and Optimize Your Genie Space

> **Note:** This project is experimental and under active development.

An LLM-powered Databricks App tool that:

1. Analyzes your Databricks Genie Space configurations against best practices.
2. Get actionable insights and recommendations to improve your Genie Space setup by labeling Genie results on your pre-defined Benchmark questions.

## Walkthrough

### Analyze Mode

<p align="left">
  <img src="images/app-intro.png" alt="Enter Genie Space ID" width="700"><br>
  <em>1) Enter your Genie Space ID or paste JSON, and then select mode (Analyze/Optimize)</em>
</p>

<p align="left">
  <img src="images/ingest.png" alt="Preview ingested data" width="700"><br>
  <em>2) Preview the ingested configuration data</em>
</p>

<p align="left">
  <img src="images/analysis-result.png" alt="Section analysis in progress" width="700"><br>
  <em>3) Analyze each section against best practices</em>
</p>

<p align="left">
  <img src="images/analysis-summary.png" alt="Final compliance summary" width="700"><br>
  <em>4) View the final compliance summary and scores</em>
</p>

### Optimize Mode

<p align="left">
  <img src="images/select-benchmarks.png" alt="Preview ingested data" width="700"><br>
  <em>1) Select benchmark questions</em>
</p>

<p align="left">
  <img src="images/generating.png" alt="Preview ingested data" width="700"><br>
  <em>2) Generating Genie responses</em>
</p>

<p align="left">
  <img src="images/labeling.png" alt="Preview ingested data" width="700"><br>
  <em>3) Label Genie responses with feedback. All feedback are aggregated and used for optimization</em>
</p>

<p align="left">
  <img src="images/optimization-results.png" alt="Preview ingested data" width="700"><br>
  <em>5) Review optimization suggestions</em>
</p>

<p align="left">
  <img src="images/create-new-space.png" alt="Preview ingested data" width="700"><br>
  <em>6) Select suggestions and click "Create New Genie" to preview a side-by-side JSON diff of the proposed configuration changes</em>
</p>

## Deployment

This app is deployed as a [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/). The frontend (React/Vite) and backend (FastAPI) are built and served together — there is no separate local dev server.

### Prerequisites

* [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) installed and authenticated (`databricks auth login`)
* A Databricks workspace with Apps enabled
* Access to a Databricks-hosted LLM endpoint (Claude Sonnet recommended)

### 1. Clone the repo

```bash
git clone <repo-url>
cd databricks-genie-workbench
```

### 2. Create the app

Create a new Databricks App via the workspace UI (**Compute > Apps > Create App**). Note the app name you choose (e.g. `genie-workbench`).

### 3. Sync local files to the workspace

```bash
databricks sync --watch . /Workspace/Users/<your-email>/genie-workbench
```

This uploads your project files to a workspace folder and watches for changes. Files listed in `.gitignore` and `.databricksignore` are excluded (e.g. `node_modules/`, `dist/`, `.env`).

### 4. Deploy the app

```bash
databricks apps deploy <app-name> \
  --source-code-path /Workspace/Users/<your-email>/genie-workbench
```

During deployment, Databricks Apps automatically:

1. Runs `npm install` (detects root `package.json`)
2. Runs `pip install -r requirements.txt`
3. Runs `npm run build` (builds the React frontend to `dist/`)
4. Starts the app via the command in `app.yaml` (`uvicorn agent_server.start_server:app`)

### 5. Configure the App

Open `app.yaml` in the workspace editor and configure the environment variables:

```yaml
env:
  # OPTIONAL: For capturing agent tracing
  - name: MLFLOW_EXPERIMENT_ID
    value: ""
  # REQUIRED: Recommend sticking with Claude Sonnet 4.5 or Opus 4.5
  - name: LLM_MODEL
    value: "databricks-claude-sonnet-4-5"
  # REQUIRED: For Optimize mode SQL execution
  - name: SQL_WAREHOUSE_ID
    value: ""
  # REQUIRED: For creating new Genie Spaces (e.g., /Workspace/Users/you@company.com/)
  - name: GENIE_TARGET_DIRECTORY
    value: ""
```

### 5. Configure user authorization scopes

The app uses OBO (On-Behalf-Of) auth so each user operates under their own identity. Add the following OAuth scopes in the Databricks Apps UI (**Compute > Apps > [app] > Edit > User Authorization > +Add Scope**):

| Scope | Purpose |
|---|---|
| `sql` | SQL warehouse queries |
| `dashboards.genie` | Genie Space API (`/api/2.0/genie/spaces/*`) |
| `serving.serving-endpoints` | LLM serving endpoint queries |
| `catalog.catalogs:read` | Unity Catalog catalog browsing |
| `catalog.schemas:read` | Unity Catalog schema browsing |
| `catalog.tables:read` | Unity Catalog table browsing |

## MLflow Tracing (Optional)

MLflow tracing logs all LLM calls and analysis steps to your Databricks workspace. To enable it:

### Creating an MLflow Experiment

1. Navigate to **Machine Learning > Experiments**
2. Click **Create Experiment**
3. Name it (e.g., `genie-space-analyzer`)
4. Leave **Artifact Location** blank (uses default)
5. Click **Create**
6. Copy the experiment ID from the URL (e.g., `https://your-workspace.cloud.databricks.com/ml/experiments/123456789`) or from the experiment details
7. Update `MLFLOW_EXPERIMENT_ID` in `app.yaml` with this ID

### Viewing Traces

1. Go to your Databricks workspace
2. Navigate to **Machine Learning > Experiments**
3. Find your experiment
4. Click on **Traces** to see all analysis traces

**Filter by session:**

```text
metadata.`mlflow.trace.session` = '<session-id>'
```

## Local Development

**Quick start:**

```bash
# Backend with hot-reload
uv run uvicorn agent_server.start_server:app --reload --port 5001

# Frontend dev server (separate terminal)
npm run dev
```

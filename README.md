# Games Social Listening

A Databricks Asset Bundle for analyzing player feedback from gaming platforms (Steam, Google Play, Reddit) using AI-powered sentiment extraction and reporting.

## Overview

This bundle provides an end-to-end solution for:
- **Ingesting** player reviews and feedback from multiple gaming platforms
- **Processing** content through AI translation and sentiment extraction
- **Analyzing** feedback with automated reporting
- **Visualizing** insights through an interactive dashboard

## Bundle Contents

### Jobs

#### Orchestration Job (`Games_Social_Listening_Demo_Job`)

The main orchestration job that runs the complete data pipeline from ingestion to dashboard refresh.

**Schedule:** Daily at 8:00 AM ET (paused by default)

**Tasks:**
<img width="1970" height="510" alt="image" src="https://github.com/user-attachments/assets/ddbec373-4352-4d27-83d9-09c9635470ac" />

| Task | Type | Description |
|------|------|-------------|
| `pull_source_content` | Notebook | Ingests player content from Steam, Google Play, or Reddit based on job parameters |
| `sentiment-extraction` | Pipeline | Triggers the DLT pipeline to process and analyze content |
| `new_game_check` | Condition | Checks if this is a new game being added (triggers report generation) |
| `summary_report_gen` | Notebook | Generates an AI-powered summary report (only runs for new games) |
| `refresh_dashboard` | Dashboard | Refreshes the Lakeview dashboard with latest data |

**Content Ingestion & Sampling:**

Ingested content is sampled:

| Setting | Value | Description |
|---------|-------|-------------|
| Max content per source | 10,000 | Maximum reviews/posts fetched per ingestion |
| Sampling threshold | 2,000 | If content exceeds this count, uniform random sampling is applied |
| Languages (Google Play) | 8 | Reviews fetched across: en, fr, es, ja, ko, zh, pt, hi |


**Job Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_content_id` | `com.nianticlabs.pokemongo` | Platform-specific identifier (e.g., Google Play package name, Steam App ID, Reddit subreddit) |
| `content_type` | `Google Play Review` | Source type: `Google Play Review`, `Steam Review`, or `Reddit Post` |
| `game_name` | `Pokemon Go` | Display name for the game |
| `update_type` | `NEW_GAME` | Set to `NEW_GAME` to add new game, or `REFRESH` to pull in new content for all games |
| `catalog` | (from bundle) | Unity Catalog catalog name |
| `schema` | (from bundle) | Schema for storing tables |

**Running with Custom Parameters:**

```bash
# Add a new game
databricks bundle run Games_Social_Listening_Demo_Job --target dev \
  --param source_content_id="730" \
  --param content_type="Steam Review" \
  --param game_name="Counter-Strike 2" \
  --param update_type="NEW_GAME"

# Refresh existing game data
databricks bundle run Games_Social_Listening_Demo_Job --target dev \
  --param update_type="REFRESH"
```

#### Weekly Summary Report Job (`Games_Social_Listening_Weekly_Summary_Report`)

A standalone job that generates weekly summary reports for all tracked games.

**Schedule:** Weekly (active)

**Tasks:**

| Task | Type | Description |
|------|------|-------------|
| `report_generator` | Notebook | Runs the weekly report generator notebook |

---

### Pipeline

#### Sentiment Extraction Pipeline (`Games_Social_Listening_Demo_Pipeline`)

A Lakeflow Declarative pipeline that processes player feedback through 4 transformation stages.

**Pipeline Flow:**

<img width="1848" height="502" alt="image" src="https://github.com/user-attachments/assets/2926c230-02b7-4426-9167-de3dcc2e0455" />

**Transformation Stages:**

| Stage | Table Created | Description |
|-------|---------------|-------------|
| **01 - AI Translation** | `feedback_content_translated` | Uses `ai_translate()` to translate all content to English |
| **02 - AI Sentiment Extraction** | `feedback_content_ai_extraction` | Uses Meta Llama 3.3 70B to extract sentiment and categorize feedback into topics (gameplay, graphics, performance, etc.) |
| **03 - Parse Sentiment** | `feedback_content_parsed` | Parses the JSON output from the LLM into structured columns with data quality expectations |
| **04 - Reporting Layer** | `feedback_content_gold`, `feedback_content_sentiment_gold` | Creates final denormalized tables optimized for dashboard queries |

---

### Dashboard

<img width="3046" height="1204" alt="image" src="https://github.com/user-attachments/assets/5eb34bda-b37d-444d-8525-512edd563c67" />


## Prerequisites

1. **Databricks CLI** (v0.218.0 or higher)
   ```bash
   # Check version
   databricks -v
   
   # Install/update via Homebrew (macOS)
   brew install databricks/tap/databricks
   ```

2. **Databricks Workspace** with:
   - Unity Catalog enabled
   - A catalog and schema for storing data
   - A SQL Warehouse for dashboard queries

3. **Authentication** configured for your workspace

4. **API Keys** for data ingestion:
   - **Reddit API**: Client ID, Client Secret, User Agent ([Create an app](https://www.reddit.com/prefs/apps))
   - **Steam API**: API Key ([Get a key](https://steamcommunity.com/dev/apikey))

## Setup Instructions

### Step 1: Clone the Repository

```bash
git clone <repository-url>
```

### Step 2: Configure Authentication

Authenticate with your Databricks workspace using OAuth:

```bash
databricks auth login --host <your-workspace-url>
```

For example:
```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

Follow the browser prompts to complete authentication.

### Step 3: Create Catalog and Schema

Create a Unity Catalog catalog and schema to store the pipeline tables:

```sql
-- Run in Databricks SQL Editor or a notebook
CREATE CATALOG IF NOT EXISTS social_listening;
CREATE SCHEMA IF NOT EXISTS social_listening.player_feedback;
```

Or use an existing catalog and create just the schema:

```sql
-- Using an existing catalog (e.g., 'main')
CREATE SCHEMA IF NOT EXISTS main.social_listening;
```

> **Note:** You'll need appropriate permissions to create catalogs/schemas. Contact your workspace admin if needed.

### Step 4: Configure Bundle Variables

Edit `databricks.yml` to set your target configuration (using the catalog/schema from Step 3):

```yaml
targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
      root_path: /Workspace/Users/your-email/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: your_catalog        # Unity Catalog catalog name
      schema: your_schema          # Schema for storing tables
      warehouse_id: your_warehouse # SQL Warehouse ID for dashboard
      prefix: your_prefix          # Prefix for resource names (e.g., "dev")
```

### Step 5: Set Up Secrets for Ingestion

Create a secret scope and add the required API keys for data ingestion:

```bash
# Create the secret scope
databricks secrets create-scope social_listening_app

# Add Reddit API credentials
databricks secrets put-secret social_listening_app reddit_client_id --string-value "YOUR_REDDIT_CLIENT_ID"
databricks secrets put-secret social_listening_app reddit_client_secret --string-value "YOUR_REDDIT_CLIENT_SECRET"
databricks secrets put-secret social_listening_app reddit_user_agent --string-value "YOUR_REDDIT_USER_AGENT"

# Add Steam API key
databricks secrets put-secret social_listening_app steam_api_key --string-value "YOUR_STEAM_API_KEY"
```

To verify secrets were created:
```bash
databricks secrets list-secrets social_listening_app
```

### Step 6: Validate the Bundle

```bash
databricks bundle validate
```

This checks your configuration for errors. You should see:
```
Validation OK!
```

### Step 7: Deploy the Bundle

```bash
databricks bundle deploy --target dev
```

This will:
- Upload source files to your workspace
- Create/update the jobs, pipeline, and dashboard
- Display "Deployment complete!" on success

### Step 8: Verify Deployment

Check deployed resources in your Databricks workspace:
- **Jobs & Pipelines** → Look for jobs with your prefix
- **SQL** → **Dashboards** → Find your dashboard

### Step 9: Run the Job to Create Tables

Before configuring the dashboard, run the orchestration job to ingest content and create the required tables.

**Quick Start - Run with default parameters:**

Simply run the job with defaults to pull in your first game (Pokemon Go from Google Play) in the UI

click **Run now** in the Databricks UI without changing any parameters.

**Custom Game - Specify parameters:**

To add a different game, provide custom parameters:

1. Open the deployed job in your Databricks workspace (**Jobs & Pipelines** → find your job with prefix)
2. Click **Run now** and set:
   - `source_content_id`: Game identifier (e.g., `com.supercell.clashofclans` for Google Play, `730` for Steam)
   - `content_type`: `Google Play Review`, `Steam Review`, or `Reddit Post`
   - `game_name`: Display name for the game
   - `update_type`: `NEW_GAME`

Wait for the job to complete successfully (this may take 10-15 minutes).

### Step 10: Configure Dashboard Data Sources

> ⚠️ **Note:** Dashboard data sources cannot be dynamically set via DABs yet (coming soon). You must manually configure them.

1. Open the deployed dashboard in your Databricks workspace
2. Click **Edit** to enter edit mode
3. Go to the **Data** tab
4. For each data source/dataset:
   - Click on the dataset
   - Update the **catalog** and **schema** to match your configuration (e.g., `your_catalog.your_schema`)
5. Click **Publish** to save changes


## Updating the Bundle

After making changes to source files or configuration:

```bash
databricks bundle deploy --target dev
```

If the dashboard has been modified in the UI (e.g., editing data sources, filters, or visualizations), use `--force` to override remote changes:

```bash
databricks bundle deploy --target dev --force
```

## Destroying the Bundle

To remove all deployed resources:

```bash
databricks bundle destroy --target dev
```

⚠️ This will delete all jobs, pipelines, and dashboards created by this bundle.

## Project Structure

```
├── databricks.yml              # Bundle configuration
├── README.md                   # This file
├── pyproject.toml              # Python project config
├── resources/                  # Resource definitions
│   ├── Games Social Listening Demo Job.job.yml
│   ├── Games Social Listening Demo Pipeline.pipeline.yml
│   ├── Games Social Listening Demo Dashboard.dashboard.yml
│   └── Games Social Listening - Weekly Summary Report.job.yml
├── src/                        # Source code
│   ├── Abstracted_Ingestion.ipynb
│   ├── 03_Weekly_Report_Generator.ipynb
│   ├── Games Social Listening Dashboard.lvdash.json
│   ├── Games Social Listening Demo Pipeline/
│   │   └── transformations/    # DLT transformation scripts
│   ├── config/                 # Configuration files
│   └── ingestion_utils/        # Ingestion helper modules
└── fixtures/                   # Test fixtures
```

## Documentation

- [Databricks Asset Bundles Overview](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Bundle Configuration Reference](https://docs.databricks.com/en/dev-tools/bundles/reference.html)
- [Databricks CLI Installation](https://docs.databricks.com/en/dev-tools/cli/install.html)

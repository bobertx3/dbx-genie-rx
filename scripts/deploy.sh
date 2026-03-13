#!/bin/bash
# =============================================================================
# Deploy Script for Databricks Apps
# =============================================================================
# This script syncs files to Databricks and deploys the app.
# Based on: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy
#
# It will:
#   1. Verify prerequisites (Databricks CLI, authentication, app.yaml)
#   2. Sync files to the workspace
#   3. Deploy (or create + deploy) the app
#
# Usage: ./scripts/deploy.sh <app-name>
# Example: ./scripts/deploy.sh genie-space-analyzer
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
info() { echo -e "${BLUE}ℹ${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
warn() { echo -e "${YELLOW}⚠${NC} $1"; }
error() { echo -e "${RED}✗${NC} $1"; exit 1; }

echo ""
echo "=========================================="
echo "  Genie Space Analyzer - Deploy to Databricks"
echo "=========================================="
echo ""

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
APP_NAME="${1:-genie-space-analyzer}"

# -----------------------------------------------------------------------------
# Step 1: Check for Databricks CLI
# -----------------------------------------------------------------------------
info "Checking for Databricks CLI..."
if command -v databricks &> /dev/null; then
    success "Databricks CLI is installed"
else
    error "Databricks CLI is not installed. Please install it first:

    # macOS
    brew tap databricks/tap
    brew install databricks

    See: https://docs.databricks.com/dev-tools/cli/install"
fi

# -----------------------------------------------------------------------------
# Step 2: Check Databricks authentication
# -----------------------------------------------------------------------------
info "Checking Databricks authentication..."
if databricks current-user me &> /dev/null; then
    DATABRICKS_USER=$(databricks current-user me 2>/dev/null | jq -r '.userName // .user_name // "unknown"')
    success "Authenticated as: $DATABRICKS_USER"
else
    error "Not authenticated with Databricks. Please run:

    databricks auth login

    Or run ./scripts/quickstart.sh to set up your environment."
fi

# -----------------------------------------------------------------------------
# Step 3: Check app.yaml exists
# -----------------------------------------------------------------------------
info "Checking app.yaml configuration..."
if [ -f "app.yaml" ]; then
    success "app.yaml found"
else
    error "app.yaml not found. Please run this script from the project root."
fi

# -----------------------------------------------------------------------------
# Step 4: Check MLFLOW_EXPERIMENT_ID is set in app.yaml
# -----------------------------------------------------------------------------
info "Checking MLflow experiment configuration..."
MLFLOW_EXP_ID=$(grep -A1 "MLFLOW_EXPERIMENT_ID" app.yaml | grep "value:" | sed 's/.*value: *"\{0,1\}\([^"]*\)"\{0,1\}/\1/' | tr -d ' ')

if [ -z "$MLFLOW_EXP_ID" ] || [ "$MLFLOW_EXP_ID" = '""' ]; then
    warn "MLFLOW_EXPERIMENT_ID is not set in app.yaml"
    echo ""
    echo "  Enter your MLflow experiment ID (or press Enter to skip):"
    read -p "  Experiment ID: " MLFLOW_EXP_ID

    # Update app.yaml if we have an experiment ID
    if [ -n "$MLFLOW_EXP_ID" ]; then
        info "Updating app.yaml with experiment ID..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' "s/\(MLFLOW_EXPERIMENT_ID\)$/\1/; /MLFLOW_EXPERIMENT_ID/{n;s/value: *\"[^\"]*\"/value: \"$MLFLOW_EXP_ID\"/;}" app.yaml
        else
            sed -i "s/\(MLFLOW_EXPERIMENT_ID\)$/\1/; /MLFLOW_EXPERIMENT_ID/{n;s/value: *\"[^\"]*\"/value: \"$MLFLOW_EXP_ID\"/;}" app.yaml
        fi
        success "Updated app.yaml with MLFLOW_EXPERIMENT_ID: $MLFLOW_EXP_ID"
    else
        warn "Skipping MLflow configuration. Tracing will be disabled."
        echo ""
    fi
else
    success "MLFLOW_EXPERIMENT_ID is configured: $MLFLOW_EXP_ID"
fi

# -----------------------------------------------------------------------------
# Step 5: Sync files to workspace
# -----------------------------------------------------------------------------
info "Syncing files to Databricks workspace..."

# Use a separate path for app source code (avoid conflict with MLflow experiments)
WORKSPACE_PATH="/Workspace/Users/$DATABRICKS_USER/apps/$APP_NAME"
info "Target path: $WORKSPACE_PATH"

# Sync the project files
databricks sync . "$WORKSPACE_PATH"

success "Files synced to $WORKSPACE_PATH"

# -----------------------------------------------------------------------------
# Step 6: Deploy (or create + deploy) the app
# -----------------------------------------------------------------------------
echo ""
info "Deploying app: $APP_NAME ..."

# Check if the app already exists
if databricks apps get "$APP_NAME" &> /dev/null; then
    info "App '$APP_NAME' exists, deploying update..."
    databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH"
else
    info "App '$APP_NAME' does not exist, creating..."
    databricks apps create "$APP_NAME"
    info "Deploying..."
    databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH"
fi

success "Deployment initiated for $APP_NAME"

# -----------------------------------------------------------------------------
# Done
# -----------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  Deployment Started!"
echo "=========================================="
echo ""
echo "Source code: $WORKSPACE_PATH"
echo ""
echo "The app uses OBO (On-Behalf-Of) authentication — users access"
echo "resources with their own Databricks permissions. No service"
echo "principal grants are needed."
echo ""
echo "Check deployment status:"
echo "  databricks apps get $APP_NAME"
echo ""
echo "View logs:"
echo "  databricks apps logs $APP_NAME"
echo ""

#!/usr/bin/env bash
set -euo pipefail

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required to install MySQL Shell on macOS." >&2
  echo "Install Homebrew from https://brew.sh, then rerun setup.sh." >&2
  exit 1
fi

brew update
brew install mysql-shell

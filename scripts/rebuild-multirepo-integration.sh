#!/usr/bin/env bash
# Build one disposable integration venv from independently resolved packages.
# This intentionally does not use a uv workspace, uv sync, or a lock file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$CORE_REPO/.integration-venv"
PYTHON="$VENV_DIR/bin/python"
cd "$CORE_REPO"

CORE_SOURCE="$CORE_REPO"
GALLERYINFO_SOURCE="index"
DOWNLOADER_SOURCE="index"
INGEST_SOURCE="index"
KOMGA_SOURCE="index"
OPDS_SOURCE="index"
HBROWSER_SOURCE="index"

usage() {
    cat <<'EOF'
Usage: scripts/rebuild-multirepo-integration.sh [--source PACKAGE=SOURCE]...

The checked-out h2hdb project is used for core by default. Every consumer is
resolved from the configured package index. SOURCE may be `index`, a local
project directory, a wheel, an archive/URL, or a Git requirement such as
`git+https://example.invalid/project.git@ref`.

Supported packages: h2hdb, h2h-galleryinfo-parser, h2hdb-downloader,
h2hdb-ingest, h2hdb-komga, h2hdb-opds, hbrowser.
EOF
}

set_source() {
    local assignment="$1"
    local package="${assignment%%=*}"
    local source="${assignment#*=}"
    if [[ "$assignment" != *=* || -z "$source" ]]; then
        echo "Invalid --source value: $assignment" >&2
        usage >&2
        exit 2
    fi
    case "$package" in
        h2hdb) CORE_SOURCE="$source" ;;
        h2h-galleryinfo-parser) GALLERYINFO_SOURCE="$source" ;;
        h2hdb-downloader) DOWNLOADER_SOURCE="$source" ;;
        h2hdb-ingest) INGEST_SOURCE="$source" ;;
        h2hdb-komga) KOMGA_SOURCE="$source" ;;
        h2hdb-opds) OPDS_SOURCE="$source" ;;
        hbrowser) HBROWSER_SOURCE="$source" ;;
        *)
            echo "Unsupported integration package: $package" >&2
            usage >&2
            exit 2
            ;;
    esac
}

while (($#)); do
    case "$1" in
        --source)
            if (($# < 2)); then
                echo "--source requires PACKAGE=SOURCE" >&2
                exit 2
            fi
            set_source "$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

INSTALL_ARGUMENTS=()
append_source() {
    local package="$1"
    local source="$2"
    if [[ "$source" == "index" ]]; then
        INSTALL_ARGUMENTS+=("$package")
    elif [[ -d "$source" ]]; then
        if [[ ! -f "$source/pyproject.toml" ]]; then
            echo "Local project has no pyproject.toml: $source" >&2
            exit 1
        fi
        INSTALL_ARGUMENTS+=(--editable "$source")
    else
        INSTALL_ARGUMENTS+=("$source")
    fi
}

append_source h2hdb "$CORE_SOURCE"
append_source h2h-galleryinfo-parser "$GALLERYINFO_SOURCE"
append_source h2hdb-downloader "$DOWNLOADER_SOURCE"
append_source h2hdb-ingest "$INGEST_SOURCE"
append_source h2hdb-komga "$KOMGA_SOURCE"
append_source h2hdb-opds "$OPDS_SOURCE"
append_source hbrowser "$HBROWSER_SOURCE"

uv venv --clear --python 3.14 "$VENV_DIR"
uv pip install --refresh --python "$PYTHON" "${INSTALL_ARGUMENTS[@]}"

"$PYTHON" "$CORE_REPO/scripts/smoke-multirepo.py"

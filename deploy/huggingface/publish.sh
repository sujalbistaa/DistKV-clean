#!/usr/bin/env bash
#
# Push this repository to a Hugging Face Space as a Docker Space.
#
#   deploy/huggingface/publish.sh <user>/<space-name>
#
# A Space is its own git repository, and it needs two files at its root
# that this repository cannot have there: a Dockerfile that builds the
# whole demo into one container (the repository's own Dockerfile builds one
# node, for compose to run five of), and a README.md carrying the Space's
# configuration in its front matter (the repository's own README is for
# people). So rather than maintaining a second copy of the source, this
# assembles a staging tree — the working tree, with those two files
# replaced — and pushes that.
#
# The push is a force-push of a single fresh commit. The Space's history is
# not interesting; what is in it is.

set -euo pipefail

SPACE="${1:-}"
if [[ -z "$SPACE" || "$SPACE" != */* ]]; then
    cat >&2 <<'USAGE'
usage: deploy/huggingface/publish.sh <user>/<space-name>

Create the Space first, at https://huggingface.co/new-space, choosing
"Docker" and the blank template. Then:

    export HF_TOKEN=hf_...        # a write token from huggingface.co/settings/tokens
    deploy/huggingface/publish.sh yourname/distkv
USAGE
    exit 2
fi

command -v rsync >/dev/null || { echo "publish: rsync is required" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

STAGING="$(mktemp -d)"
trap 'rm -rf "$STAGING"' EXIT

echo "publish: assembling $SPACE from $REPO_ROOT"

# The working tree, not the last commit: this should push what you have in
# front of you, including anything not committed yet. The exclusions are
# things the image builds for itself, and pushing them would at best waste
# everyone's time and at worst ship a mac's node_modules to a linux build.
rsync -a \
    --exclude '.git/' \
    --exclude 'node_modules/' \
    --exclude 'web/dist/' \
    --exclude 'bin/' \
    --exclude '*.test' \
    --exclude '.DS_Store' \
    ./ "$STAGING/"

cp deploy/huggingface/Dockerfile "$STAGING/Dockerfile"
cp deploy/huggingface/README.md "$STAGING/README.md"
chmod +x "$STAGING/deploy/huggingface/start.sh"

REMOTE="https://huggingface.co/spaces/$SPACE"
if [[ -n "${HF_TOKEN:-}" ]]; then
    # The token goes in the URL for this one push and is never written to
    # disk: the staging tree is deleted on exit, and its git config with it.
    REMOTE="https://user:${HF_TOKEN}@huggingface.co/spaces/$SPACE"
fi

cd "$STAGING"
git init -q -b main
git add -A
git -c user.email=publish@localhost -c user.name=publish \
    commit -q -m "DistKV cluster console"

echo "publish: pushing to https://huggingface.co/spaces/$SPACE"
git push -q --force "$REMOTE" main

cat <<EOF

publish: done.

  Space:  https://huggingface.co/spaces/$SPACE
  Live:   https://$(echo "$SPACE" | tr '/' '-' | tr '[:upper:]' '[:lower:]').hf.space

The first build takes a few minutes — it compiles the console and three Go
binaries. Watch it under the "Logs" tab; "Running" means the cluster is up.
EOF

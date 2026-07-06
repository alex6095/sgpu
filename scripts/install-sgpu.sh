#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_dir="${1:-$HOME/.local/bin}"

mkdir -p "$install_dir"
ln -sf "$repo_root/bin/sgpu" "$install_dir/sgpu"
chmod +x "$repo_root/bin/sgpu"

echo "Installed: $install_dir/sgpu"
case ":$PATH:" in
  *":$install_dir:"*) ;;
  *)
    echo ""
    echo "Add this to your shell profile if needed:"
    echo "  export PATH=\"$install_dir:\$PATH\""
    ;;
esac

echo ""
echo "Try:"
echo "  sgpu           # interactive TUI"
echo "  sgpu once      # one-shot dashboard"
echo "  sgpu stats     # usage report"

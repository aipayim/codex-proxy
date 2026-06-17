#!/bin/bash
DIR="$(dirname "$(readlink -f "$0")")"
echo "Editing keys.json — paste your keys (one per line), Ctrl+D to save"
echo "Current keys:"
python3 -c "
import json
with open('$DIR/keys.json') as f:
    keys = json.load(f)
for i, k in enumerate(keys, 1):
    print(f'  {i}. {k[:15]}...')
"
echo "---"
readarray -t new_keys
if [ ${#new_keys[@]} -gt 0 ]; then
  json="["
  for i in "${!new_keys[@]}"; do
    if [ -n "${new_keys[$i]}" ]; then
      [ $i -gt 0 ] && json+=", "
      json+="\"${new_keys[$i]}\""
    fi
  done
  json+="]"
  echo "$json" > "$DIR/keys.json"
  echo "Saved ${#new_keys[@]} keys"
  echo "Restart proxy: kill \$(lsof -ti:3456) && node \"$DIR/proxy.js\" &"
fi

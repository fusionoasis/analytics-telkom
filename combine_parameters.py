import json
import os
import re
from typing import List, Dict, Any

# Root is the directory of this script
ROOT = os.path.dirname(os.path.abspath(__file__))
PARAMS_DIR = os.path.join(ROOT, "parameters")
OUTPUT_FILE = os.path.join(PARAMS_DIR, "all_parameters.json")


def find_parameters_files(directory: str) -> List[str]:
    files = []
    for name in os.listdir(directory):
        if not name.lower().endswith('.json'):
            continue
        if name == os.path.basename(OUTPUT_FILE):
            continue
        files.append(os.path.join(directory, name))
    return sorted(files)


def extract_sink_folder(p_rel_url: str) -> str:
    # Expected pattern: .../data/<folder>/<file>
    m = re.search(r"/data/([^/]+)/", p_rel_url)
    if m:
        return m.group(1)
    # Fallback: best-effort split
    parts = p_rel_url.split('/')
    if 'data' in parts:
        idx = parts.index('data')
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return ""


def transform_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    p_rel_url = entry.get('p_rel_url', '')
    p_sink_file = entry.get('p_sink_file', '')
    p_sink_folder = extract_sink_folder(p_rel_url)

    # Preserve desired key order: p_rel_url, p_sink_folder, p_sink_file
    transformed = {
        'p_rel_url': p_rel_url,
        'p_sink_folder': p_sink_folder,
        'p_sink_file': p_sink_file,
    }
    return transformed


def load_json_array(path: str) -> List[Dict[str, Any]]:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if not isinstance(data, list):
            raise ValueError(f"Expected a JSON array in {path}")
        return data


def main() -> None:
    os.makedirs(PARAMS_DIR, exist_ok=True)

    param_files = find_parameters_files(PARAMS_DIR)
    combined: List[Dict[str, Any]] = []

    for pf in param_files:
        try:
            entries = load_json_array(pf)
        except Exception as e:
            raise RuntimeError(f"Failed reading {pf}: {e}")

        for entry in entries:
            combined.append(transform_entry(entry))

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as out:
        json.dump(combined, out, indent=2)

    print(f"Wrote {len(combined)} entries to {OUTPUT_FILE}")


if __name__ == '__main__':
    main()

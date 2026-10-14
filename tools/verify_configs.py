"""Validate declared configuration contracts, not historical newsletter stores."""
from __future__ import annotations

import json
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIGS = ("channels.json", "cybermed_channels.json", "youtube_only.json",
           "cybermed_selection.json", "cybermed_foamed_sources.json", "cybermed_model_eval_cases.json")


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise ValueError("duplicate configuration key: " + str(key))
        result[key] = value
    return result


class UniqueYaml(yaml.BaseLoader):
    # BaseLoader preserves GitHub's YAML `on` key (unlike YAML 1.1 booleans).
    def construct_mapping(self, node, deep=False):
        return unique_pairs([(self.construct_object(k, deep=deep), self.construct_object(v, deep=deep)) for k, v in node.value])


def load_json(path):
    def nonfinite(value): raise ValueError("nonfinite configuration number")
    return json.loads(path.read_text(), object_pairs_hook=unique_pairs, parse_constant=nonfinite)


def validate_config(name, data):
    if name in {"channels.json", "cybermed_channels.json", "youtube_only.json"}:
        if not isinstance(data, dict) or not isinstance(data.get("topic_buckets"), list): raise ValueError("topic_buckets must be a list")
        for bucket in data["topic_buckets"]:
            if not isinstance(bucket, dict) or not isinstance(bucket.get("channels"), list): raise ValueError("channels must be a list")
            for channel in bucket["channels"]:
                if not isinstance(channel, dict) or not isinstance(channel.get("name"), str): raise ValueError("named channel required")
                field = "query" if channel.get("source") == "pubmed" else "url"
                if not isinstance(channel.get(field), str) or not channel[field]: raise ValueError("channel source locator required")
    elif name == "cybermed_selection.json":
        if not isinstance(data, dict) or not isinstance(data.get("selection"), dict): raise ValueError("selection object required")
        tiers = data["selection"].get("tiers")
        if not isinstance(tiers, dict) or any(not isinstance(v, list) or not all(isinstance(x, str) for x in v) for v in tiers.values()): raise ValueError("journal tiers must be lists of strings")
    elif name == "cybermed_foamed_sources.json":
        if not isinstance(data, list) or any(not isinstance(v, dict) or not isinstance(v.get("name"), str) or not isinstance(v.get("type"), str) for v in data): raise ValueError("named typed sources required")
    elif name == "cybermed_model_eval_cases.json":
        if not isinstance(data, dict) or data.get("schema_version") != 1 or not isinstance(data.get("cases"), list): raise ValueError("model evaluation case list required")
        ids = [case.get("case_id") for case in data["cases"]]
        if any(not isinstance(value, str) or not value for value in ids) or len(set(ids)) != len(ids): raise ValueError("unique case IDs required")


def verify(root=ROOT):
    for name in CONFIGS: validate_config(name, load_json(root / "data" / name))
    files = sorted((root / ".github/workflows").glob("*.yml"))
    for path in files:
        data = yaml.load(path.read_text(), Loader=UniqueYaml)
        if not isinstance(data, dict) or not data.get("name") or not isinstance(data.get("on"), dict) or not isinstance(data.get("jobs"), dict): raise ValueError("workflow name/events/jobs required: " + path.name)
    print(f"Validated {len(CONFIGS)} JSON configurations and {len(files)} workflows; historical stores untouched")


if __name__ == "__main__": verify()

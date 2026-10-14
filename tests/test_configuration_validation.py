import importlib.util
import json
from pathlib import Path

import pytest

spec=importlib.util.spec_from_file_location('verify_configs',Path(__file__).resolve().parents[1]/'tools/verify_configs.py')
validation=importlib.util.module_from_spec(spec); spec.loader.exec_module(validation)


def test_checked_configuration_contracts():
    validation.verify()
    tiers=validation.load_json(validation.ROOT/'data/cybermed_selection.json')['selection']['tiers']
    assert tiers['tier3_pain_strict']==['Pain','Reg Anesth Pain Med']


def test_duplicate_json_keys_are_rejected_in_nested_configuration(tmp_path):
    path=tmp_path/'bad.json'; path.write_text('{"selection":{"tiers":{"pain":[],"pain":["other"]}}}')
    with pytest.raises(ValueError,match='duplicate'): validation.load_json(path)


def test_duplicate_yaml_keys_are_rejected_and_github_on_remains_a_string():
    import yaml
    with pytest.raises(ValueError,match='duplicate'):
        yaml.load('jobs:\n  check: {}\n  check: {}\n',Loader=validation.UniqueYaml)
    assert 'on' in yaml.load('on:\n  pull_request: {}\n',Loader=validation.UniqueYaml)


def test_configuration_shape_failure_is_not_tolerated():
    with pytest.raises(ValueError): validation.validate_config('channels.json',{'topic_buckets':[{'channels':[{'name':'missing locator'}]}]})

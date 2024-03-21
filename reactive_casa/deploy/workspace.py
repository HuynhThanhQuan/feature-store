import os
import pathlib
import yaml

import logging
logger = logging.getLogger(__name__)


def _structure_project_folders():
    os.makedirs('./log', exist_ok=True)
    os.makedirs('./data', exist_ok=True)
    os.makedirs('./model', exist_ok=True)


def _read_config(args):
    config = {}
    # Read default config file
    with open(f'./config/default.yaml', 'r') as f:
        default_cfg = yaml.safe_load(f)
        logger.debug(f'Default Config: {default_cfg}')
        config.update(default_cfg)

    # Update input config
    input_cfg = vars(args)
    logger.debug(f'User config: {input_cfg}')
    config.update(input_cfg)

    # Read ML config jobs
    with open(f'./config/{args.job}.yaml', 'r') as f:
        ml_config = yaml.safe_load(f)
        logger.debug(f'ML Config: {ml_config}')
        config.update(ml_config)
    logger.info(f'Final Config: {config}')
    return config



def setup(args):
    assert os.path.exists('./config'), 'System Error! Not found config folder, please copy it'
    _structure_project_folders()
    config = _read_config(args)
    return config
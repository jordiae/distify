from hydra import compose
from hydra.core.config_store import ConfigStore
from hydra.core.hydra_config import HydraConfig
from hydra.utils import to_absolute_path
from omegaconf import OmegaConf
import os
from app import run
import hydra
import time
from typing import Any
from dataclasses import dataclass


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("checkpoint")
    args = parser.parse_args()
    print('Resuming')
    #output_dir = to_absolute_path(args.checkpoint)
    output_dir = args.checkpoint
    original_overrides = OmegaConf.load(os.path.join(output_dir, ".hydra", "overrides.yaml"))
    # current_overrides = HydraConfig.get().overrides.task

    hydra_config = OmegaConf.load(os.path.join(output_dir, ".hydra", "hydra.yaml"))
    # getting the config name from the previous job.
    config_name = hydra_config.hydra.job.config_name
    # concatenating the original overrides with the current overrides
    overrides = original_overrides  # + current_overrides
    # compose a new config from scratch
    hydra.initialize(config_path=os.path.join(output_dir, ".hydra"))#(config_path=os.path.join(output_dir, ".hydra"))
    cfg = compose("config", overrides=overrides, return_hydra_config=True)
    cfg.hydra.run.dir = output_dir
    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    new_config_path = os.path.join(output_dir, ".hydra")
    new_config_file = f"restore-{timestamp}.yaml"
    with open(os.path.join(new_config_path, new_config_file), 'w') as f:
        OmegaConf.save(cfg, f)

    @hydra.main(config_path=new_config_path, config_name=new_config_file)
    def resume():
        # sweep/multirun doesnt work!
        run(cfg.frequency)
    resume()


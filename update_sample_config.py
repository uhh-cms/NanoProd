import os
import sys
import yaml
import json
from tqdm import tqdm

from argparse import ArgumentParser, RawDescriptionHelpFormatter


def update_config(sample_dict, meta_info):
    pbar_samples = tqdm(meta_info)
    for sample in pbar_samples:
        pbar_samples.set_description(f"Updating time stamps for {sample}")
        timestamps = meta_info[sample]["time_stamps"]

        try:
            sample_dict[sample]["timestamps"] = timestamps
        except Exception as e:
            print(e)
            print("skipping")

def main(*args, configpath, new_configpath, meta_info_jsons, **kwargs):

    # open sample config
    with open(configpath) as f:
        sample_dict = yaml.load(f, yaml.Loader)
    
    for info in meta_info_jsons:
        with open(info) as f:
            this_meta_info = json.load(f)
        
        update_config(sample_dict, this_meta_info)
    
    with open(new_configpath, "w") as f:
        yaml.dump(sample_dict, f)

def parse_arguments():

    parser = ArgumentParser()

    parser.add_argument("-b", "--base",
        help=" ".join(
            """
            path to config that is to be updated. Has to be im yaml format!    
            """.split()
        ),
        dest="configpath",
        metavar="path/to/config.yaml",
        type=str
    )

    parser.add_argument("-o", "--output",
        help=" ".join(
            """
            path where updated config is to be written.
            """.split()
        ),
        dest="new_configpath",
        metavar="path/to/new_config.yaml",
        type=str,
    )

    parser.add_argument("meta_info_jsons",
        help=" ".join(
            """
            path to json files containing info about fully done jobs
            """.split()
        ),
        metavar="path/to/summary*.json",
        nargs="+",
        type=str,
    )

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = parse_arguments()
    main(
        **vars(args),
    )

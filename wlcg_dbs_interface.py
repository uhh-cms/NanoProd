import os
import sys
import yaml
import json
import uproot as up
import numpy as np

try:
    import gfal2
except ImportError as e:
    print("WARNING: could not import gfal2")
    print(e)
    print("gfal will be disabled!")
    gfal2 = None



from subprocess import PIPE, Popen
from itertools import chain
from typing import Any
from tqdm import tqdm
from RunKit.envToJson import get_cmsenv


class WLCGInterface(object):
    def __init__(self,
        # wlcg_path: str or None=None,
        # route_url: str or None=None,
        verbosity: int=0,
    ):
        # self.wlcg_path = wlcg_path
        # self.route_url = route_url
        self.__verbosity = verbosity
        # setup gfal context
        try:
            # create gfal context
            if not gfal2:
                raise NotImplementedError("Cannot load remote file without gfal2 module!")

            self.gfal_context = gfal2.creat_context()
        except NotImplementedError as e:
            print(e)
            self.gfal_context = None
        self.dbs_api = self.setup_dbs_api()
        self.xrtd_redirectors = [
            "cms-xrd-global.cern.ch",
            "xrootd-cms.infn.it",
            "cmsxrootd.fnal.gov",
            "xrootd-cms.infn.it"
        ]
        # cmssw environment information in case crab needs to be called
        self.cmsswEnv = None


    @property
    def verbosity(self):
        return self.__verbosity
    

    @verbosity.setter
    def verbosity(self, val: int):
        self.__verbosity = val

    

    def getCmsswEnv(self):
        if self.cmsswEnv is None:
            cmssw_path = os.environ['DEFAULT_CMSSW_BASE']
            self.cmsswEnv = get_cmsenv(cmssw_path, crab_env=True)
            self.cmsswEnv['X509_USER_PROXY'] = os.environ['X509_USER_PROXY']
            self.cmsswEnv['HOME'] = os.environ['HOME'] if 'HOME' in os.environ else os.getcwd()
        return self.cmsswEnv


    def setup_dbs_api(
        self,
        cms_dbs_url: str="https://cmsweb.cern.ch/dbs/prod/global/DBSReader",
    ):
        # setup dbs search
        try:
            from dbs.apis.dbsClient import DbsApi
            
            return DbsApi(url=cms_dbs_url)
        except:
            print("WARNING: Could not find dbs3 module. Did you install it with")
            print("python3 -m pip install --user dbs3-client")
            print("?")
            print("Will use dasgoclient as fallback instead")
            return None

    def get_remote_file(
        self,
        filepath:str ,
        target: str,
        # route_url: str="root://cms-xrd-global.cern.ch",
        enforce_success: bool=False,
    ):  
        target = os.path.abspath(target)
        for route_url in self.xrtd_redirectors:
            url=f"root://{route_url}//{filepath}"

            # copy remote file
            try:
                self.gfal_context.filecopy(
                    self.gfal_context.transfer_parameters(),
                    url,
                    f"file://{target}"
                )
                break
            except Exception as e:
                print(e)

        if enforce_success and not os.path.exists(target):
            raise ValueError(f"Unable to copy file '{url}' to '{target}'")

    def move_file_to_remote(
        self,
        local_file: str,
        target_file: str,
        route_url: str="root://cms-xrd-global.cern.ch",
        cleanup: bool=False,
    ):
        local_file_path = f"file://{os.path.abspath(local_file)}"
        target_dir = os.path.dirname(target_file)

        if route_url != None:
            remote_url = f"{route_url}//{target_file}"
            wlcg_target_dir = f"{route_url}//{target_dir}"

        else:
            remote_url = target_file
            wlcg_target_dir = target_dir

        self.gfal_context.mkdir_rec(wlcg_target_dir, 0)

        self.gfal_context.filecopy(
            self.gfal_context.transfer_parameters(),
            local_file_path,
            remote_url
        )

        if cleanup:
            os.remove(local_file)

    def load_remote_output(
        self,
        wlcg_path: str,
    ) -> list[str]:
        """Function to load file paths from a remote WLCG target *wlcg_path*.
        First, the function checks for the gfal2 module. If gfal is loaded correctly,
        the list of files from the remote directly *wlcg_path* is loaded.
        If any of these steps fail, an empty list is returned

        Args:
            wlcg_path (str):    Path to the WLCG remote target, which consists of the
                                WLCG prefix and the actual directory on the remote
                                site (constructed from global wlcg_template)

        Returns:
            list[str]:  List of files in remote target *wlcg_path*. Defaults to
                        empty list.
        """
        try:
            if self.gfal_context:
                # load list of files
                filelist = self.gfal_context.listdir(wlcg_path)
                return [os.path.join(wlcg_path, x) for x in filelist]
            else:
                if self.verbosity >= 1:
                    print(f"unable to load files from {wlcg_path}, skipping")
                    from IPython import embed; embed()
        except Exception as e:
            print(f"unable to load files from {wlcg_path}, skipping")
        return []

    def load_events_from_file(self, remote_file: str, treename: str="Events"):
        try:
            from IPython import embed; embed()
            f = up.open({remote_file: treename})
            
            return f.num_entries
        except OSError as oserror:
            print(oserror)
            print("open IPython shell for debugging")
            from IPython import embed; embed()
        return 0

    def load_events(self, remote_files: set[str], treename: str="Events"):
        return np.sum([
            self.load_events_from_file(remote_file=path, treename=treename) 
            for path in remote_files
        ])

    def compare_events(
        self,
        relevant_ids,
        job_outputs,
        input_map,
        event_lookup,
        name_template="output_{id}.tar",
    ):
        event_comparison = list()
        pbar_ids = tqdm(relevant_ids)
        
        for id in pbar_ids:
            pbar_ids.set_description(f"Comparing events for job {id}")
            relevant_job_outputs = set()
            relevant_job_outputs = set(filter(
                    lambda x: x.endswith(name_template.format(id=id)), 
                    job_outputs
                ))
            all_events = sum([event_lookup.get(x, 0) for x in input_map[id]])

            job_events = self.load_events(remote_files=relevant_job_outputs)

            if all_events != job_events:
                rel_diff = (all_events-job_events)/all_events if not all_events == 0 else 0

                event_comparison.append({
                    "lfns": input_map[id],
                    "all_events": all_events,
                    "saved_events": job_events,
                    "rel_diff": rel_diff,
                })
        return event_comparison


    def check_job_outputs(
        self,
        collector_set: set[str],
        input_map: dict[str, list[str]],
        job_details: dict[str, dict],
        state: str="failed",
        job_outputs: set or None=None,
        wlcg_prefix: str="",
        xrd_prefix: str="",
        event_lookup: dict or None=None,
        event_comparison_container: list or None=None,
        verbosity: int=0,
        name_template: str="output_{id}.tar",
    ) -> None:
        """Function to collect information about jobs in *job_details*.
        First, all job ids with state *state* are retrieved from *job_details*.
        Then, the *collector_set* is filled with information depending on the 
        *state*.
        If *state* is 'failed', the *collector_set* is filled with paths to files
        that correspond to failed jobs and thus should not exist.
        If *state* is 'finished', the *collector_set* is filled with lfns that
        were successfully processed. In this case, an additional check whether
        a lfn is already marked as done is performed, and raises an error if
        a marked lfn is supposed to be added again.
        If the set *job_outputs* is neither None nor empty, the file paths are
        matched to the job ids with the current state. Only IDs with output files
        are considered further.

        Args:
            collector_set (set):    set to be filled with information, depending on 
                                    *state* (see description above)
            input_map (dict): Dictionary of format {job_id: list_of_lfns}
            job_details (dict): Dictionary containing the status of the jobs of
                                format {job_id: ADDITIONAL_INFORMATION}.
                                *ADDITIONAL_INFORMATION* is a dict that must contain
                                the keyword 'State'.
            state (str, optional):  State to select the relevant job ids.
                                    Must be either "failed" or "finished".
                                    Defaults to "failed".
            job_outputs (set, optional):    if a set of output files is given,
                                            only job ids with output files are
                                            considered as relevant. Defaults to None

        Raises:
            ValueError: If a lfn is already marked as done but is associated with
                        a done job again, the ValueError is raised.
        """

        relevant_ids = set(filter(
            lambda x: job_details[x]["State"] == state,
            job_details
        ))

        # if there are paths to the job outputs available, only select ids that
        # actually have an output
        if isinstance(job_outputs, set) and not len(job_outputs) == 0:
            relevant_ids = set(filter(
                lambda x: any(path.endswith(name_template.format(id=x)) for path in job_outputs), 
                relevant_ids
            ))

        # for state "failed", collect output files that should not be there
        if state == "failed":
            collector_set.update(filter(
                lambda x: any(x.endswith(name_template.format(id=x)) for id in relevant_ids), 
                job_outputs
            ))
        # if state is finished, safe the done lfns (if the output of the job is also 
        # available)
        elif state == "finished":
            
            lfns = set(chain.from_iterable([input_map[x] for x in relevant_ids]))
            
            # first check if a lfn is already marked as done - this should not happen
            
            if event_lookup:
                # all following steps use XROOTD to contact specific remote files, 
                # so update prefix accordingly
                event_comparison_container += self.compare_events(
                    relevant_ids=relevant_ids,
                    job_outputs=set([x.replace(wlcg_prefix, xrd_prefix) for x in job_outputs]),
                    input_map=input_map,
                    event_lookup=event_lookup
                )
                
            overlap = collector_set.intersection(lfns)
            if len(overlap) != 0:
                if verbosity == 0:
                    msg = " ".join(f"""
                        {len(overlap)} LFNs are already marked as 'done' but
                        have come up here again. This should not happen
                    """.split())
                    # raise ValueError(msg)
                else:
                    overlap_string = "\n".join(overlap)
                    raise ValueError(f"""
                    The following lfns were already marked as done:
                    {overlap_string}

                    This should not happen!
                    """)
                
            collector_set.update(lfns)


    def load_das_key(
        self,
        sample_name: str,
        sample_config: str,
        verbosity: int=0
    ) -> str or None:
        """Small function to extract the DAS key for sample *sample_name* 
        from the *sample_config*. First, the *sample_config*
        is opened (has to be in yaml format!). Afterwards, the entry *sample_name*
        is extracted. This entry should be a dictionary itself, which should contain
        the key 'miniAOD' with the DAS key for this sample.

        Args:
            sample_name (str): Name of the sample as provided in the sample config
            sample_config (str):    path to the sample_config.yaml file containing 
                                    the information mentioned above.

        Returns:
            str or None: If successfull, this function returns the DAS key, else None
        """    
        das_key = None
        # open the sample config
        with open(sample_config) as f:
            sample_dict = yaml.load(f, yaml.Loader)

        # look up information for sample_name
        sample_info = sample_dict.get(sample_name, dict())
        # if there is no sample information, exit here
        if len(sample_info) == 0:
            if verbosity >= 1:
                print(f"WARNING: Unable to load information for sample '{sample_name}'")
            return das_key
        key = sample_info.get("miniAOD", None)
        if not key:
            key = sample_info.get("inputDataset", None)
        return key

    def get_campaign_name(self, das_key: str=None, verbosity: int=0) -> str:
        """small function to translate the sample name attributed by the 
        crabOverseer to the original MC campaign name. The original 
        campaign name is then extracted from the DAS key. If any of these steps
        fails, the fucntion returns an empty string

        Args:
            das_key (str):  DAS key in str format. Any other format will return
                            the default value of '""'.

        Returns:
            str: if successful, returns the original campaign name, else ""
        """    
        sample_campaign = ""
        # load information about the original miniAOD DAS key
        if not isinstance(das_key, str):
            if verbosity >= 1:
                msg=" ".join(f"""
                WARNING: Unable to load campaign name from das key of type
                '{type(das_key)}'
                """.split())
                print(msg)
            return sample_campaign

        # original campaign name is the first part of the DAS key
        sample_campaign = das_key.split("/")[1]
        
        return sample_campaign

    def load_valid_file_list(self, das_key: str) -> dict[str: Any]:
        # load the file list for this dataset
        try:
            file_list = self.dbs_api.listFiles(dataset=das_key, detail=1)
        except Exception as e:
            print("Encounter exception:")
            print(e)
            from IPython import embed
            embed()
            raise e
        # by default, this list contains _all_ files (also LFNs that are not
        # reachable) so filter out broken files
        file_list = list(filter(
            lambda x: x["is_file_valid"] == True,
            file_list
        ))
        return file_list
    
    def create_event_lookup(
        self,
        das_key: str
    ) -> dict[str, int]:
        if self.dbs_api:
            dbs_file_list = self.load_valid_file_list(das_key=das_key)
            return {
                x["logical_file_name"]: x["event_count"] for x in dbs_file_list
            }
        return dict()

    def get_dbs_lfns(self, das_key: str) -> set[str]:
        """Small function to load complete list of valid LFNs for dataset with
        DAS key *das_key*. Only files where the flag 'is_file_valid' is True
        are considered. Returns set of lfn paths if successful, else an empty set

        Args:
            das_key (str): key in CMS DBS service for the dataset of interest

        Returns:
            set[str]: Set of LFN paths
        """    
        # initialize output set as empty
        output_set = set()

        # if the api for the dbs interface was initialized sucessfully, we can
        # load the files
        if self.dbs_api:
            file_list = self.load_valid_file_list(das_key=das_key)
            output_set = set([x["logical_file_name"] for x in file_list])
        return output_set 

    def get_das_information(
        self,
        das_key: str,
        relevant_info: str="num_file",
        default: int=-1,
    ) -> int:
        allowed_modes="file_size num_event num_file".split()
        if not relevant_info in allowed_modes:
            raise ValueError(f"""Could not load information '{relevant_info}'
            because it's not part of the allowed modes: file_size, num_event, num_file
            """)
        output_value = default

        # execute DAS query for sample with *das_key*
        process = Popen(
            [f"dasgoclient --query '{das_key}' -json"], 
            shell=True, stdin=PIPE, stdout=PIPE
        )
        # load output of query
        output, stderr = process.communicate()
        # from IPython import embed; embed()
        # output is a string of form list(dict()) and can be parsed with
        # the json module
        try:
            das_infos = json.loads(output)
        except Exception as e:
            # something went wrong in the parsing, so just return the default
            return output_value
        # not all dicts have the same (relevant) information, so go look for the
        # correct entry in list. Relevant information for us is the total
        # number of LFNs 'nfiles'
        relevant_values = list(set(
            y.get(relevant_info) 
            # only consider entries in DAS info list with dataset information
            for x in das_infos if "dataset" in x.keys() 
            # only consider those elements in dataset info that also have 
            # the relevant information
            for y in x["dataset"] if relevant_info in y.keys()
        ))
        # if this set is has more than 1 or zero entries, something went wrong
        # when obtaining the relevant information, so return the default value

        # if the set has exactly one entry, we were able to extract the relevant
        # information, so return it accordingly
        if len(relevant_values) == 1:
            output_value = relevant_values[0]
        return output_value

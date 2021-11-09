import json
import os


def load_configs_from_files(file_names):
    file_names = object_to_list(file_names)
    if len(file_names) == 0:
        return dict()
    config = load_config_by_name(file_names[0])
    for arg in file_names[1:]:
        cur_config = load_config_by_name(arg)
        config.update(cur_config)
    return config


def object_to_list(obj):
    if isinstance(obj, list):
        return obj
    return [obj]


def load_config_by_name(conf_path):
    """ Load configuration
    Args:
        conf_path: the path to the configuration file
    Returns:
        A dict object of configuration
    """
    if conf_path.endswith(".json"):
        return json.load(open(conf_path))
    elif conf_path.endswith(".yaml"):
        return yaml_loader(conf_path)
    return dict()


def yaml_loader(filename):
    import yaml
    try:
        from yaml import CSafeLoader as SafeLoader
    except ImportError:
        from yaml import SafeLoader

    with open(filename) as fh:
        return yaml.load(fh, Loader=SafeLoader) or {}


def extract_files_name(folder_path):
    """ extract configure parameters from all the python files in the folder.
        Specifically, extract the word after self.conf and self.conf.get

        Args:
            folder_path: the folder path that contains multiple codes file.
        Returns:
             config_name_set: string set that contains all configuration names.
        """

    # ------get all the .py files name from the folder_path.------

    # to store files in a list
    files_paths_list = []
    # iterate all the sub_folders and files in folder_path.
    a = os.listdir(folder_path)
    for (dir_path, _, cur_file_names) in os.walk(folder_path):
        # dir_path: current path,
        # cur_dir_names: sub folders' names in the current path,
        # cur_file_names: files' names in the current path

        for cur_file_name in cur_file_names:
            if '.py' in cur_file_name:
                # join the dir_path and current file name to get the path.
                # then append to the files_list
                files_paths_list.append(os.path.join(dir_path, cur_file_name))
    return files_paths_list

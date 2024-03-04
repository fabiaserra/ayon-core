import collections

import ayon_api

from ayon_core.client import (
    get_subsets,
    get_last_versions,
)


def get_last_versions_for_instances(
    project_name, instances, use_value_for_missing=False
):
    """Get last versions for instances by their folder path and product name.

    Args:
        project_name (str): Project name.
        instances (list[CreatedInstance]): Instances to get next versions for.
        use_value_for_missing (Optional[bool]): Missing values are replaced
            with negative value if True. Otherwise None is used. -2 is used
            for instances without filled folder or product name. -1 is used
            for missing entities.

    Returns:
        dict[str, Union[int, None]]: Last versions by instance id.
    """

    output = {
        instance.id: -1 if use_value_for_missing else None
        for instance in instances
    }
    product_names_by_folder_path = collections.defaultdict(set)
    instances_by_hierarchy = {}
    for instance in instances:
        folder_path = instance.data.get("folderPath")
        product_name = instance.product_name
        if not folder_path or not product_name:
            if use_value_for_missing:
                output[instance.id] = -2
            continue

        (
            instances_by_hierarchy
            .setdefault(folder_path, {})
            .setdefault(product_name, [])
            .append(instance)
        )
        product_names_by_folder_path[folder_path].add(product_name)

    product_names = set()
    for names in product_names_by_folder_path.values():
        product_names |= names

    if not product_names:
        return output

    folder_entities = ayon_api.get_folders(
        project_name,
        folder_paths=product_names_by_folder_path.keys(),
        fields={"id", "path"}
    )
    folder_paths_by_id = {
        folder_entity["id"]: folder_entity["path"]
        for folder_entity in folder_entities
    }
    if not folder_paths_by_id:
        return output

    subset_docs = get_subsets(
        project_name,
        asset_ids=folder_paths_by_id.keys(),
        subset_names=product_names,
        fields=["_id", "name", "parent"]
    )
    subset_docs_by_id = {}
    for subset_doc in subset_docs:
        # Filter subset docs by subset names under parent
        folder_id = subset_doc["parent"]
        folder_path = folder_paths_by_id[folder_id]
        product_name = subset_doc["name"]
        if product_name not in product_names_by_folder_path[folder_path]:
            continue
        subset_docs_by_id[subset_doc["_id"]] = subset_doc

    if not subset_docs_by_id:
        return output

    last_versions_by_product_id = get_last_versions(
        project_name,
        subset_docs_by_id.keys(),
        fields=["name", "parent"]
    )
    for subset_id, version_doc in last_versions_by_product_id.items():
        subset_doc = subset_docs_by_id[subset_id]
        folder_id = subset_doc["parent"]
        folder_path = folder_paths_by_id[folder_id]
        _instances = instances_by_hierarchy[folder_path][subset_doc["name"]]
        for instance in _instances:
            output[instance.id] = version_doc["name"]

    return output


def get_next_versions_for_instances(project_name, instances):
    """Get next versions for instances by their folder path and product name.

    Args:
        project_name (str): Project name.
        instances (list[CreatedInstance]): Instances to get next versions for.

    Returns:
        dict[str, Union[int, None]]: Next versions by instance id. Version is
            'None' if instance has no folder path or product name.
    """

    last_versions = get_last_versions_for_instances(
        project_name, instances, True)

    output = {}
    for instance_id, version in last_versions.items():
        if version == -2:
            output[instance_id] = None
        elif version == -1:
            output[instance_id] = 1
        else:
            output[instance_id] = version + 1
    return output

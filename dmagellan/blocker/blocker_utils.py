def get_attrs_to_project(key, block_attr, output_attrs):
    proj_attrs = [key, block_attr]
    if output_attrs is not None:
        if not isinstance(output_attrs, list):
            output_attrs = [output_attrs]
        output_attrs = [c for c in output_attrs if c not in proj_attrs]
        proj_attrs.extend(output_attrs)
    return proj_attrs

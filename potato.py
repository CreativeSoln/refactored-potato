def _parse_param_safe(
    self,
    param_el: ET.Element,
    parentType: str,
    parentPath: str,
    layerName: str,
    serviceShortName: str,
):
    try:
        return self.parse_param(
            param_el,
            parentType,
            parentPath,
            layerName,
            serviceShortName,
            {}, {}, {}, {}, {}, {}
        )
    except Exception:
        # HARD fallback – always return something
        return OdxParam(
            id=f"{layerName}::{serviceShortName}::{parentType}::{get_text_local(param_el,'SHORT-NAME')}",
            shortName=get_text_local(param_el, "SHORT-NAME"),
            longName=get_text_local(param_el, "LONG-NAME"),
            description=get_text_local(param_el, "DESC"),
            semantic=get_text_local(param_el, "SEMANTIC"),
            parentType=parentType,
            parentName=parentPath,
            layerName=layerName,
            serviceShortName=serviceShortName,
            attrs=get_all_attrs(param_el),
        )

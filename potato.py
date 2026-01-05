def add_param_recursive(parent_item: QTreeWidgetItem, p: OdxParam) -> bool:
    nonlocal param_count_visible

    pname = getattr(p, "shortName", "") or "(param)"
    semantic = getattr(p, "semantic", "") or ""
    third = cell_value(p)

    # Determine match
    match_self = True
    if self._filter_text:
        match_self = any(
            self._filter_text in (t or "").lower()
            for t in (pname, semantic, third)
        )

    # Always create the node first (CRITICAL)
    p_item = QTreeWidgetItem([pname, semantic, third])
    p_item.setFlags(
        p_item.flags()
        | Qt.ItemFlag.ItemIsUserCheckable
        | Qt.ItemFlag.ItemIsSelectable
    )
    p_item.setCheckState(0, Qt.CheckState.Unchecked)
    p_item.setData(0, Qt.ItemDataRole.UserRole, p)
    parent_item.addChild(p_item)

    # Render children
    any_child_visible = False
    for c in getattr(p, "children", []) or []:
        if add_param_recursive(p_item, c):
            any_child_visible = True

    # Apply filter visibility
    if self._filter_text and not match_self and not any_child_visible:
        parent_item.removeChild(p_item)
        return False

    param_count_visible += 1
    return True

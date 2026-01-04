

def show_service_details(self, s: OdxService) -> None:
    self._add_detail("Service ShortName", getattr(s, "shortName", ""))
    self._add_detail("Service LongName", getattr(s, "longName", ""))
    self._add_detail("Semantic", getattr(s, "semantic", ""))
    self._add_detail("Request DID", getattr(s, "requestDidHex", ""))
    sid = getattr(s, "sid", None)
    if isinstance(sid, int):
        self._add_detail("SID", f"0x{sid:02X}")
    self._add_detail("Description", getattr(s, "description", ""))
    self._add_detail("Info Text", getattr(s, "infoText", ""))
    self._add_detail("Addressing", getattr(s, "addressing", ""))


def copy_current_did(self) -> None:
    """
    Copies the DID (requestDidHex) of the selected Service node to clipboard, if available.
    If a Param is selected, it will search its ancestor Service.
    """
    item = self.tree.currentItem()
    if not item:
        self.sb.showMessage("Select a service to copy DID")
        return

    obj = item.data(0, Qt.ItemDataRole.UserRole)
    service: Optional[OdxService] = obj if isinstance(obj, OdxService) else None

    if service is None:
        parent = item.parent()
        while parent and service is None:
            pobj = parent.data(0, Qt.ItemDataRole.UserRole)
            if isinstance(pobj, OdxService):
                service = pobj
                break
            parent = parent.parent()

    if not service:
        self.sb.showMessage("No service selected for DID")
        return

    did_hex = getattr(service, "requestDidHex", "") or ""
    if not did_hex:
        self.sb.showMessage("Selected service has no requestDidHex")
        return

    QGuiApplication.clipboard().setText(did_hex)
    self.sb.showMessage(f"Copied DID: {did_hex}")


def populate_tree(self, initial_build: bool = False) -> None:
    """
    Builds the QTreeWidget from self.database based on current filter state.
    If initial_build is True, also initializes the filter combos.
    """
    if not self.database:
        return

    # Clear only the tree (do not collapse user expansions on filter)
    self.tree.clear()
    param_count_visible = 0
    layer_count = 0

    # 1) Collect variants (ECU + Base)
    variants: List[OdxLayer] = []
    variants.extend(getattr(self.database, "ecuVariants", []) or [])
    variants.extend(getattr(self.database, "baseVariants", []) or [])

    # 2) Initialize filter combos (once)
    if initial_build:
        # Variant combo
        self.cmb_variant.blockSignals(True)
        self.cmb_variant.clear()
        self.cmb_variant.addItem("All Variants", userData="")
        for v in variants:
            sn = getattr(v, "shortName", "")
            self.cmb_variant.addItem(sn, userData=sn)
        self.cmb_variant.blockSignals(False)

        # Semantic combo
        self.cmb_semantic.blockSignals(True)
        self.cmb_semantic.clear()
        self.cmb_semantic.addItem("All semantics", userData="")
        semantics: Set[str] = set()
        for v in variants:
            for s in getattr(v, "services", []) or []:
                sem = getattr(s, "semantic", "")
                if sem:
                    semantics.add(sem)
        for sem in sorted(semantics):
            self.cmb_semantic.addItem(sem, userData=sem)
        self.cmb_semantic.blockSignals(False)

        # SID combo (optional / legacy – only if parser provides it)
        self.cmb_sid.blockSignals(True)
        self.cmb_sid.clear()
        self.cmb_sid.addItem("All SIDs", userData=None)
        sids: Set[int] = set()
        for v in variants:
            for s in getattr(v, "services", []) or []:
                sid = getattr(s, "sid", None)
                if isinstance(sid, int):
                    sids.add(sid)
        for sid in sorted(sids):
            self.cmb_sid.addItem(f"0x{sid:02X}", userData=sid)
        self.cmb_sid.blockSignals(False)

    # 3) Read current filters
    self._filter_variant = self.cmb_variant.currentData()
    self._filter_semantic = self.cmb_semantic.currentData()
    self._filter_sid_int = self.cmb_sid.currentData()
    self._filter_text = self.search.text().strip().lower()

    # Helpers
    def param_info(p: OdxParam) -> str:
        parts: List[str] = []
        byte_pos = getattr(p, "bytePosition", "")
        bit_len = getattr(p, "bitLength", "")
        base = getattr(p, "baseDataType", "") or getattr(p, "physicalBaseType", "")
        coded_const = getattr(p, "codedConstValue", "")
        phys_const = getattr(p, "physConstValue", "")
        const = coded_const or phys_const

        if byte_pos:
            parts.append(f"BytePos={byte_pos}")
        if bit_len:
            parts.append(f"BitLen={bit_len}")
        if base:
            parts.append(f"BaseType={base}")
        if const:
            parts.append(f"Const={const}")
        return " | ".join(parts)

    def m_type_label(kind: str) -> str:
        return {
            "Request": "REQUEST",
            "Positive Response": "POS_RESPONSE",
            "Negative Response": "NEG_RESPONSE",
        }.get(kind, kind.upper())

    # Recursive param renderer (STRUCTURE / TABLE-ROW safe)
    def add_param_recursive(parent_item: QTreeWidgetItem, p: OdxParam) -> bool:
        nonlocal param_count_visible
        pname = getattr(p, "shortName", "") or "(param)"
        semantic = getattr(p, "semantic", "") or ""
        # Prefer `value` per your parser; fallback to `displayValue` for backward compatibility
        value = getattr(p, "value", "") or getattr(p, "displayValue", "") or ""
        info = param_info(p)

        # Text filter
        if self._filter_text:
            if not any(
                self._filter_text in (t or "").lower()
                for t in (pname, semantic, value, info)
            ):
                # Still recurse, in case a child matches (keeps structure nodes)
                any_child_visible = False
                for c in getattr(p, "children", []) or []:
                    if add_param_recursive(parent_item, c):
                        any_child_visible = True
                return any_child_visible

        p_item = QTreeWidgetItem([pname, semantic, value if value else info])
        p_item.setFlags(
            p_item.flags()
            | Qt.ItemFlag.ItemIsUserCheckable
            | Qt.ItemFlag.ItemIsSelectable
        )
        p_item.setCheckState(0, Qt.CheckState.Unchecked)
        p_item.setData(0, Qt.ItemDataRole.UserRole, p)
        parent_item.addChild(p_item)
        param_count_visible += 1

        child_visible = False
        for c in getattr(p, "children", []) or []:
            if add_param_recursive(p_item, c):
                child_visible = True

        # We added the parent item; return True (visible)
        return True

    # 4) Build tree
    for v in variants:
        vname = getattr(v, "shortName", "")
        if self._filter_variant and vname != self._filter_variant:
            continue

        v_item = QTreeWidgetItem([
            vname or "(variant)",
            getattr(v, "layerType", ""),
            getattr(v, "description", ""),
        ])
        v_item.setFlags(v_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        v_item.setCheckState(0, Qt.CheckState.Unchecked)
        v_item.setData(0, Qt.ItemDataRole.UserRole, v)
        self.tree.addTopLevelItem(v_item)
        layer_count += 1

        for s in getattr(v, "services", []) or []:
            if self._filter_semantic and getattr(s, "semantic", "") != self._filter_semantic:
                continue
            sid = getattr(s, "sid", None)
            if self._filter_sid_int is not None and sid != self._filter_sid_int:
                continue

            service_texts = [
                getattr(s, "shortName", ""),
                getattr(s, "semantic", ""),
                getattr(s, "longName", ""),
                getattr(s, "description", ""),
                getattr(s, "requestDidHex", ""),  # DID-centric
            ]
            service_passes_text = (
                not self._filter_text
                or any(self._filter_text in (t or "").lower() for t in service_texts)
            )
            did_text = getattr(s, "requestDidHex", "") or ""
            s_item = QTreeWidgetItem([
                f"{getattr(s, 'shortName', '')} ({did_text})" if did_text else getattr(s, "shortName", ""),
                getattr(s, "semantic", ""),
                getattr(s, "description", "") or "",
            ])
            s_item.setFlags(
                s_item.flags()
                | Qt.ItemFlag.ItemIsUserCheckable
                | Qt.ItemFlag.ItemIsSelectable
            )
            s_item.setCheckState(0, Qt.CheckState.Unchecked)
            s_item.setData(0, Qt.ItemDataRole.UserRole, s)
            v_item.addChild(s_item)

            messages: List[Tuple[str, Any]] = []
            req = getattr(s, "request", None)
            if req:
                messages.append(("Request", req))
            for r in getattr(s, "posResponses", []) or []:
                messages.append(("Positive Response", r))
            for r in getattr(s, "negResponses", []) or []:
                messages.append(("Negative Response", r))

            has_visible_params_any_message = False

            for kind, msg in messages:
                m_item = QTreeWidgetItem([
                    getattr(msg, "shortName", "") or m_type_label(kind),
                    m_type_label(kind),
                    getattr(msg, "longName", "") or "",
                ])
                m_item.setFlags(
                    m_item.flags()
                    | Qt.ItemFlag.ItemIsUserCheckable
                    | Qt.ItemFlag.ItemIsSelectable
                )
                m_item.setCheckState(0, Qt.CheckState.Unchecked)
                m_item.setData(0, Qt.ItemDataRole.UserRole, msg)
                s_item.addChild(m_item)

                has_visible_params_this_message = False
                for p in getattr(msg, "params", []) or []:
                    if add_param_recursive(m_item, p):
                        has_visible_params_this_message = True

                has_visible_params_any_message = (
                    has_visible_params_any_message or has_visible_params_this_message
                )

            # Hide only if service doesn't match text AND none of its messages had visible params
            if not service_passes_text and not has_visible_params_any_message:
                s_item.setHidden(True)

    # 5) Final UI updates
    if initial_build:
        self.tree.expandToDepth(1)
    self.lbl_layers.setText(f"{layer_count} layers")
    self.lbl_selected.setText(f"{len(self.selectedParams)}/{param_count_visible} selected")

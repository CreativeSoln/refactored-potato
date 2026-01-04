def populate_tree(self, initial_build: bool = False) -> None:
    """
    Builds the QTreeWidget from self.database based on current filter state.
    If initial_build is True, also initializes the filter combos.
    """
    if not self.database:
        return

    self.tree.clear()
    param_count_visible = 0
    layer_count = 0

    # -------------------------------------------------
    # 1) Collect variants (ECU + Base)
    # -------------------------------------------------
    variants: List[OdxLayer] = []
    variants.extend(getattr(self.database, "ecuVariants", []) or [])
    variants.extend(getattr(self.database, "baseVariants", []) or [])

    # -------------------------------------------------
    # 2) Initialize filter combos (once)
    # -------------------------------------------------
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

    # -------------------------------------------------
    # 3) Read current filters
    # -------------------------------------------------
    self._filter_variant = self.cmb_variant.currentData()
    self._filter_semantic = self.cmb_semantic.currentData()
    self._filter_sid_int = self.cmb_sid.currentData()
    self._filter_text = self.search.text().strip().lower()

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------
    def param_info(p: OdxParam) -> str:
        parts = []
        if p.bytePosition:
            parts.append(f"BytePos={p.bytePosition}")
        if p.bitLength:
            parts.append(f"BitLen={p.bitLength}")
        base = p.baseDataType or p.physicalBaseType
        if base:
            parts.append(f"BaseType={base}")
        if p.codedConstValue or p.physConstValue:
            parts.append(f"Const={p.codedConstValue or p.physConstValue}")
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

        pname = p.shortName or "(param)"
        semantic = p.semantic or ""
        value = p.value or ""
        info = param_info(p)

        # Text filter
        if self._filter_text:
            if not any(self._filter_text in (t or "").lower()
                       for t in (pname, semantic, value, info)):
                return False

        p_item = QTreeWidgetItem([pname, semantic, value])
        p_item.setFlags(p_item.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsSelectable)
        p_item.setCheckState(0, Qt.CheckState.Unchecked)
        p_item.setData(0, Qt.ItemDataRole.UserRole, p)
        parent_item.addChild(p_item)

        param_count_visible += 1

        child_visible = False
        for c in getattr(p, "children", []) or []:
            if add_param_recursive(p_item, c):
                child_visible = True

        return True or child_visible

    # -------------------------------------------------
    # 4) Build tree
    # -------------------------------------------------
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
            if self._filter_semantic and s.semantic != self._filter_semantic:
                continue

            sid = getattr(s, "sid", None)
            if self._filter_sid_int is not None and sid != self._filter_sid_int:
                continue

            service_texts = [
                s.shortName,
                s.semantic,
                s.longName,
                s.description,
                s.requestDidHex,
            ]
            service_passes_text = (
                not self._filter_text
                or any(self._filter_text in (t or "").lower() for t in service_texts)
            )

            did_text = s.requestDidHex or ""
            s_item = QTreeWidgetItem([
                f"{s.shortName} ({did_text})" if did_text else s.shortName,
                s.semantic,
                s.description or "",
            ])
            s_item.setFlags(s_item.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsSelectable)
            s_item.setCheckState(0, Qt.CheckState.Unchecked)
            s_item.setData(0, Qt.ItemDataRole.UserRole, s)
            v_item.addChild(s_item)

            messages: List[Tuple[str, Any]] = []
            if s.request:
                messages.append(("Request", s.request))
            for r in s.posResponses or []:
                messages.append(("Positive Response", r))
            for r in s.negResponses or []:
                messages.append(("Negative Response", r))

            has_visible_params_any_message = False

            for kind, msg in messages:
                m_item = QTreeWidgetItem([
                    msg.shortName or m_type_label(kind),
                    m_type_label(kind),
                    msg.longName or "",
                ])
                m_item.setFlags(m_item.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsSelectable)
                m_item.setCheckState(0, Qt.CheckState.Unchecked)
                m_item.setData(0, Qt.ItemDataRole.UserRole, msg)
                s_item.addChild(m_item)

                has_visible_params_this_message = False
                for p in msg.params or []:
                    if add_param_recursive(m_item, p):
                        has_visible_params_this_message = True

                has_visible_params_any_message |= has_visible_params_this_message

            if not service_passes_text and not has_visible_params_any_message:
                s_item.setHidden(True)

    # -------------------------------------------------
    # 5) Final UI updates
    # -------------------------------------------------
    if initial_build:
        self.tree.expandToDepth(1)

    self.lbl_layers.setText(f"{layer_count} layers")
    self.lbl_selected.setText(f"{len(self.selectedParams)}/{param_count_visible} selected")

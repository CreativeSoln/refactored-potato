# ---------- POS RESPONSES ----------
pos_responses: List[OdxMessage] = []

# 1) POS-RESPONSE-REF (authoritative)
if pos_ref_ids:
    for rid in pos_ref_ids:
        rr = pos_resp_map.get(rid)
        if not rr:
            continue

        rr_copy = self._clone_message(rr)
        root_path = f"{svc_short}.{rr_copy.shortName or 'PosResponse'}"

        # CRITICAL
        self._prefix_path(rr_copy.params, root_path)
        self._annotate_service_name(rr_copy.params, svc_short)

        pos_responses.append(rr_copy)

# 2) Inline POS-RESPONSE (only if no refs)
elif inline_pos:
    for el in inline_pos:
        rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_pos"
        root_path = f"{svc_short}.{rshort}"

        rparams: List[OdxParam] = []
        for p_el in findall_descendants(el, "PARAM"):
            rp = self._try_parse_param(
                p_el,
                "POS_RESPONSE",
                root_path,
                layer_short,
                svc_short,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if rp:
                rparams.append(rp)

        # CRITICAL
        self._prefix_path(rparams, root_path)
        self._annotate_service_name(rparams, svc_short)

        pos_responses.append(
            OdxMessage(
                id=get_attr(el, "ID"),
                shortName=rshort,
                longName=get_text_local(el, "LONG-NAME"),
                params=rparams,
            )
        )

# ---------- NEG RESPONSES ----------
neg_responses: List[OdxMessage] = []

if neg_ref_ids:
    for rid in neg_ref_ids:
        rr = neg_resp_map.get(rid)
        if not rr:
            continue

        rr_copy = self._clone_message(rr)
        root_path = f"{svc_short}.{rr_copy.shortName or 'NegResponse'}"

        self._prefix_path(rr_copy.params, root_path)
        self._annotate_service_name(rr_copy.params, svc_short)

        neg_responses.append(rr_copy)

elif inline_neg:
    for el in inline_neg:
        rshort = get_text_local(el, "SHORT-NAME") or f"{svc_short}_neg"
        root_path = f"{svc_short}.{rshort}"

        rparams: List[OdxParam] = []
        for p_el in findall_descendants(el, "PARAM"):
            rp = self._try_parse_param(
                p_el,
                "NEG_RESPONSE",
                root_path,
                layer_short,
                svc_short,
                dop_by_id,
                dop_by_sn,
                dop_meta_by_id,
                struct_by_id,
                struct_by_sn,
                table_by_id,
            )
            if rp:
                rparams.append(rp)

        self._prefix_path(rparams, root_path)
        self._annotate_service_name(rparams, svc_short)

        neg_responses.append(
            OdxMessage(
                id=get_attr(el, "ID"),
                shortName=rshort,
                longName=get_text_local(el, "LONG-NAME"),
                params=rparams,
            )
        )

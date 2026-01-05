  # ---- RESPONSE BINDINGS (FIX) ----
        pos_ref_ids = [
            get_attr(r, "ID-REF")
            for r in find_children(svc_el, "POS-RESPONSE-REF")
            if get_attr(r, "ID-REF")
        ]
        neg_ref_ids = [
            get_attr(r, "ID-REF")
            for r in find_children(svc_el, "NEG-RESPONSE-REF")
            if get_attr(r, "ID-REF")
        ]
        inline_pos = find_children(svc_el, "POS-RESPONSE")
        inline_neg = find_children(svc_el, "NEG-RESPONSE")

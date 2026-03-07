from __future__ import annotations


def _normalize_corr_group_key(value: object) -> str:
    return str(value).strip().upper().replace(" ", "")


def _resolve_delay_map_for_groups(delay_map_raw: dict, corr_groups: list[str]) -> tuple[dict[str, float], list[str]]:
    if not isinstance(delay_map_raw, dict):
        return {}, []

    normalized_groups = {_normalize_corr_group_key(group): str(group) for group in corr_groups}
    subsystem_lookup: dict[str, list[str]] = {}
    for group in corr_groups:
        group_text = str(group)
        if "::" in group_text:
            subsystem = _normalize_corr_group_key(group_text.split("::", 1)[1])
            subsystem_lookup.setdefault(subsystem, []).append(group_text)

    resolved: dict[str, float] = {}
    unknown_keys: list[str] = []
    for raw_key, raw_value in delay_map_raw.items():
        target_group = normalized_groups.get(_normalize_corr_group_key(raw_key))
        raw_key_text = str(raw_key)
        if target_group is None and "::" in raw_key_text:
            subsystem = _normalize_corr_group_key(raw_key_text.split("::", 1)[1])
            candidates = subsystem_lookup.get(subsystem, [])
            if len(candidates) == 1:
                target_group = candidates[0]

        if target_group is None:
            unknown_keys.append(raw_key_text)
            continue

        try:
            resolved[target_group] = max(float(raw_value), 0.0)
        except (TypeError, ValueError):
            continue

    return resolved, unknown_keys

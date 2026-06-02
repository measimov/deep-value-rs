# Disabled Tushare Endpoint Inventory

The inventory YAML files under `tushare_mirror/endpoint_configs/inventory/`
are classification stubs for future Tushare API families. They are not active
endpoint configs and are not copied into `_catalog/endpoints` by
`init-catalog`.

Inventory stubs must include:

- api_name
- endpoint_kind
- planner_kind
- execution_status: disabled
- reason_disabled
- required_infra
- risk_level
- notes

Inventory files are intentionally separate from executable endpoint configs.
They can be inspected by infrastructure reports, but they must not be fetchable,
planned by mirror-run, or included in executable mirror scopes until a later
phase explicitly enables an endpoint with tests, bounded planning, and execution
policy approval.

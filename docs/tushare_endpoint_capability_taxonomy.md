# Tushare Endpoint Capability Taxonomy

Endpoint configs are normalized into a capability model before they are loaded
into the catalog. The model classifies how an endpoint may be planned without
granting permission to execute new endpoint families.

Required capability fields:

- api_name
- family
- market
- domain
- endpoint_kind
- volume_class
- planner_kind
- permission_class
- partition_template
- primary_date_field
- supported_params
- default_fields
- probe params
- probe fields
- pagination_mode
- date_strategy
- code_strategy
- period_strategy
- object_strategy
- pit_safety
- execution_status

Allowed endpoint_kind values:

- reference_snapshot
- calendar
- daily_bar
- daily_metric
- event
- constituent
- company_governance
- financial_statement
- financial_indicator
- macro
- fund
- index
- futures
- option
- hk_us
- text_news
- object_document
- minute_bar
- tick
- realtime
- unknown

Allowed planner_kind values:

- single_snapshot
- date_backfill
- calendar_backfill
- explicit_dates
- code_list
- code_date_matrix
- period
- code_period_matrix
- object_index
- object_download
- bucketed_intraday
- realtime_poll
- unsupported

Current low-risk A-share endpoints are executable only because their existing
mirror/backfill commands already enforce bounded planning, max-job limits,
snapshot validation, and backup checks. New or future endpoint families should
be classified first and remain disabled until planner support and execution
policy allow them.

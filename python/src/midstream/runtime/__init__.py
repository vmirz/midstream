"""Runtime naming, binding, and template helpers live here."""

from midstream.runtime.bindings import build_runtime_binding
from midstream.runtime.naming import (
	resolve_runtime_app_name,
	retired_runtime_components,
	runtime_component_name,
	runtime_component_names,
	runtime_consumer_group_name,
)
from midstream.scale import (
	DEFAULT_REPLICAS,
	DEFAULT_SIZING_RULES,
	DEFAULT_STORAGE_SIZE,
	InvalidScaleError,
	ScaleRecommendation,
	ScaleSizingRules,
	normalize_replicas,
	normalize_storage_size,
	recommend_replicas,
	recommend_scale,
)
from midstream.runtime.templates import (
	build_dedicated_runtime_template_input,
	iter_rendered_resources,
	render_dedicated_runtime_resources,
	spec_hash,
)

__all__ = [
	"DEFAULT_REPLICAS",
	"DEFAULT_SIZING_RULES",
	"DEFAULT_STORAGE_SIZE",
	"InvalidScaleError",
	"ScaleRecommendation",
	"ScaleSizingRules",
	"build_dedicated_runtime_template_input",
	"build_runtime_binding",
	"iter_rendered_resources",
	"normalize_replicas",
	"normalize_storage_size",
	"recommend_replicas",
	"recommend_scale",
	"render_dedicated_runtime_resources",
	"resolve_runtime_app_name",
	"retired_runtime_components",
	"runtime_component_name",
	"runtime_component_names",
	"runtime_consumer_group_name",
	"spec_hash",
]
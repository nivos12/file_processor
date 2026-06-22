"""
Step registry — maps step name strings to Step implementations.

Adding a new step: create the class in its own module, then add one line here.
The pipeline executor resolves step names against this dict at runtime.
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.steps.base import Step

# Lazy imports: we populate the dict after defining it so that importing
# registry.py doesn't force all step modules to load at once.
def _build_registry() -> dict[str, type["Step"]]:
    from app.steps.validate import ValidateStep
    from app.steps.transform import TransformStep
    from app.steps.filter import FilterStep
    from app.steps.convert import ConvertStep
    from app.steps.enrich import EnrichStep
    from app.steps.compress import CompressStep
    from app.steps.notify import NotifyStep

    return {
        "validate": ValidateStep,
        "transform": TransformStep,
        "filter": FilterStep,
        "convert": ConvertStep,
        "enrich": EnrichStep,
        "compress": CompressStep,
        "notify": NotifyStep,
    }


# Module-level dict: populated on first access pattern via __getattr__ would
# be over-engineered; just build it eagerly when the module loads.
# All step modules are small so this is cheap.
STEP_REGISTRY: dict[str, type["Step"]] = {}


def _init() -> None:
    global STEP_REGISTRY
    STEP_REGISTRY.update(_build_registry())


_init()

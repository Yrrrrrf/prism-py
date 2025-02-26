"""Core utilities for the Forge API generation framework."""

from forge.core.config import ForgeConfig
from forge.core.logging import Logger, log, color_palette

__all__ = ["ForgeConfig", "Logger", "log", "color_palette"]
# the __all__ variable is used to define what symbols get exported when the module is imported
# this means that when someone imports forge.core, they can access ForgeConfig, Logger, log, and color_palette\

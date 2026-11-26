from typing import (
    Any,
    Dict,
    Type,
    ClassVar,
    Optional,
    TypeVar,
)
from prefect.blocks.core import Block
import os
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined


T = TypeVar("T", bound="EnvBlock")


class EnvBlock(Block):
    # Keep the prefix as a model field so subclasses can override it safely.
    prefix: str | None = None

    # Singleton instance cache
    _instance: ClassVar[Optional[Any]] = None

    @staticmethod
    def retrieve_default_value(model_field: FieldInfo, env_var_name: str) -> Any:
        if model_field.default not in (None, PydanticUndefined):
            return model_field.default
        if not model_field.is_required():
            return None
        raise ValueError(f"Environment variable {env_var_name} is required.")

    @classmethod
    def resolve_prefix(cls) -> str:
        """
        Returns the resolved prefix for the subclass:
        - Use subclass-defined prefix if present
        - Otherwise fallback to class name
        - Returned value is upper cased
        """
        model_fields = cls.model_fields
        prefix_field = model_fields.get("prefix")
        default_prefix = (
            prefix_field.default if prefix_field else None
        )

        prefix = default_prefix or cls.__name__
        return prefix.upper()

    @classmethod
    async def from_env(cls: Type[T], env_vars: Optional[dict[str, str]] = None) -> T:
        """
        Load an instance of the subclass from environment variables.

        If the environment variable `<PREFIX>_CREDENTIAL_BLOCK_NAME` is set, the method
        loads the corresponding Prefect block using `cls.load()` and returns it.

        Otherwise, this method inspects the subclass's annotated attributes and attempts
        to construct the instance by reading each attribute's value from environment variables.
        Each environment variable must be named `<PREFIX>_<ATTRIBUTE>`, where `PREFIX` is
        either the class-level `prefix` or the subclass name, uppercased.

        Pydantic validates and converts supplied strings to the annotated types. If a required
        environment variable is missing and no default is defined in the subclass, a `ValueError`
        is raised. The result is cached and reused on subsequent calls.

        Example:
            For a subclass `MyCreds`:

                class MyCreds(EnvBlock):
                    token: str
                    url: str

            Set the environment variables:
                MYCREDS_TOKEN=abc123
                MYCREDS_URL=https://example.com

            Then call:
                creds = await MyCreds.from_env()

        Returns:
            An instance of the subclass with attributes populated from environment variables.

        Raises:
            ValueError: If a required environment variable is missing or cannot
            be converted to the expected type.
        """
        # Return cached instance if already created
        if cls._instance is not None:
            return cls._instance

        # If no env_vars dict passed in use os.environ
        if env_vars is None:
            env_vars = dict(os.environ)

        resolved_prefix = cls.resolve_prefix()

        # Attempt to load a Prefect block by name from an environment variable
        block_name = env_vars.get(f"{resolved_prefix}_CREDENTIAL_BLOCK_NAME")
        if block_name is not None:
            block = await cls.load(block_name)  # type: ignore
            cls._instance = block
            return block

        # Otherwise, construct the block from environment variables
        field_values: Dict[str, Any] = {}

        for field, model_field in cls.model_fields.items():
            # Skip private/internal fields
            if field.startswith("_"):
                continue

            # Form the expected environment variable name
            env_var_name = f"{resolved_prefix}_{field.upper()}"
            raw_value = env_vars.get(env_var_name)

            # Use class-level default if env var is not set
            if raw_value is None:
                field_values[field] = cls.retrieve_default_value(
                    model_field, env_var_name
                )

            else:
                field_values[field] = raw_value

        # Let Pydantic parse strings, including booleans, unions and secrets.
        instance = cls(**field_values)
        cls._instance = instance
        return instance

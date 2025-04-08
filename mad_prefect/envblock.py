from typing import Any, Dict, Type, ClassVar, Optional, TypeVar
from prefect.blocks.core import Block
import inspect
import os
from pydantic import SecretStr


T = TypeVar("T", bound="EnvBlock")


class EnvBlock(Block):
    # Optional prefix to customize environment variable names
    prefix: ClassVar[Optional[str]] = None

    # Singleton instance cache
    _instance: ClassVar[Optional[Any]] = None

    @classmethod
    async def from_env(cls: Type[T]) -> T:
        """
        Load an instance of the subclass from environment variables.

        If the environment variable `<PREFIX>_CREDENTIAL_BLOCK_NAME` is set, the method
        loads the corresponding Prefect block using `cls.load()` and returns it.

        Otherwise, this method inspects the subclass's annotated attributes and attempts
        to construct the instance by reading each attribute's value from environment variables.
        Each environment variable must be named `<PREFIX>_<ATTRIBUTE>`, where `PREFIX` is
        either the class-level `prefix` or the subclass name, uppercased.

        Values are cast to the annotated types, including support for `SecretStr`. If a required
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

        # Introspect the subclass to access declared fields and defaults
        sig = inspect.signature(cls)

        # Determine the prefix: use 'prefix' class attribute if set in the subclass, or default to the class name
        downstream_prefix_param = sig.parameters.get("prefix")
        downstream_prefix = (
            downstream_prefix_param.default
            if downstream_prefix_param is not None
            and downstream_prefix_param.default is not inspect.Parameter.empty
            else None
        )
        cls.prefix = downstream_prefix or cls.__name__
        normalized_prefix = cls.prefix.upper()  # type: ignore

        # Attempt to load a Prefect block by name from an environment variable
        block_name = os.getenv(f"{normalized_prefix}_CREDENTIAL_BLOCK_NAME")
        if block_name is not None:
            block = await cls.load(block_name)  # type: ignore
            cls._instance = block
            return block

        # Otherwise, construct the block from environment variables
        field_values: Dict[str, Any] = {}

        for field, field_type in cls.__annotations__.items():
            # Skip private/internal fields
            if field.startswith("_"):
                continue

            # Form the expected environment variable name
            env_var = f"{normalized_prefix}_{field.upper()}"
            raw_value = os.getenv(env_var)

            # Use class-level default if env var is not set
            if raw_value is None:
                param = sig.parameters.get(field)
                if param is not None and param.default is not inspect.Parameter.empty:
                    field_values[field] = param.default
                    continue
                else:
                    raise ValueError(f"Environment variable {env_var} is required.")

            # Convert raw string to the appropriate type
            try:
                if field_type == SecretStr:
                    field_values[field] = SecretStr(raw_value)
                else:
                    field_values[field] = field_type(raw_value)
            except Exception as e:
                raise ValueError(
                    f"Error converting {env_var} value '{raw_value}' to {field_type}: {e}"
                )

        # Instantiate and cache the block
        instance = cls(**field_values)
        cls._instance = instance
        return instance

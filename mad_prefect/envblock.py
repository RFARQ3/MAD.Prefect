from typing import Any, Dict, Type, ClassVar, Optional, TypeVar
from prefect.blocks.core import Block
import inspect
import os

from pydantic import SecretStr

T = TypeVar("T", bound="EnvBlock")


class EnvBlock(Block):
    prefix: ClassVar[Optional[str]] = None
    _instance: ClassVar[Optional[Any]] = None

    @classmethod
    async def from_env(cls: Type[T]) -> T:
        if cls._instance is not None:
            return cls._instance

        # Use the subclass signature to access default values.
        sig = inspect.signature(cls)

        # If prefix attribute exists in subclass use this as cls.prefix else use subclass name
        downstream_prefix_param = sig.parameters.get("prefix")
        downstream_prefix = (
            downstream_prefix_param.default
            if downstream_prefix_param is not None
            and downstream_prefix_param.default is not inspect.Parameter.empty
            else None
        )

        cls.prefix = downstream_prefix or cls.__name__

        normalized_prefix = cls.prefix.upper()  # type: ignore

        # Check for a pre-registered block name from the environment.
        block_name = os.getenv(f"{normalized_prefix}_CREDENTIAL_BLOCK_NAME")
        if block_name is not None:
            block = await cls.load(block_name)  # type: ignore
            cls._instance = block
            return block

        field_values: Dict[str, Any] = {}

        for field, field_type in cls.__annotations__.items():
            if field.startswith("_"):
                continue

            env_var = f"{normalized_prefix}_{field.upper()}"
            raw_value = os.getenv(env_var)

            # If the env var is missing, try to use the default value from the signature.
            if raw_value is None:
                param = sig.parameters.get(field)
                if param is not None and param.default is not inspect.Parameter.empty:
                    field_values[field] = param.default
                    continue
                else:
                    raise ValueError(f"Environment variable {env_var} is required.")

            try:
                # Convert to SecretStr if needed; otherwise, convert using the type.
                if field_type == SecretStr:
                    field_values[field] = SecretStr(raw_value)
                else:
                    field_values[field] = field_type(raw_value)
            except Exception as e:
                raise ValueError(
                    f"Error converting {env_var} value '{raw_value}' to {field_type}: {e}"
                )

        instance = cls(**field_values)
        cls._instance = instance
        return instance

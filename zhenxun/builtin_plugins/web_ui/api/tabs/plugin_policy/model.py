from pydantic import BaseModel, Field


class AccountPolicyUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    block_plugins: list[str] = Field(default_factory=list)
    block_tasks: list[str] = Field(default_factory=list)


class PolicyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=255)
    source_bot_id: str | None = Field(default=None, max_length=255)
    source_revision: str | None = Field(default=None, min_length=64, max_length=64)
    block_plugins: list[str] = Field(default_factory=list)
    block_tasks: list[str] = Field(default_factory=list)
    bind_bot_ids: list[str] = Field(default_factory=list)
    expected_revisions: dict[str, str] = Field(default_factory=dict)


class PolicyUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=255)
    block_plugins: list[str] = Field(default_factory=list)
    block_tasks: list[str] = Field(default_factory=list)


class PolicyBindingUpdate(BaseModel):
    policy_id: int = Field(gt=0)
    bot_ids: list[str] = Field(min_length=1)
    expected_revisions: dict[str, str]


class PolicyCopy(BaseModel):
    source_bot_id: str = Field(min_length=1, max_length=255)
    source_revision: str = Field(min_length=64, max_length=64)
    target_bot_ids: list[str] = Field(min_length=1)
    expected_revisions: dict[str, str]

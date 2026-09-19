from pydantic import BaseModel, Field

from zhenxun.utils.enum import BlockType


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


class ScopedPolicyImport(AccountPolicyUpdate):
    bot_id: str = Field(min_length=1, max_length=191)
    platform_scope: str = Field(min_length=1, max_length=32)
    group_id: str = Field(min_length=1, max_length=191)
    channel_id: str = Field(default="", max_length=191)
    forced_plugins: list[str] = Field(default_factory=list)
    forced_tasks: list[str] = Field(default_factory=list)


class AccountPolicyImport(AccountPolicyUpdate):
    bot_id: str
    policy_id: int | None = None


class TemplatePolicyImport(PolicyUpdate):
    id: int


class GlobalFeatureImport(BaseModel):
    status: bool
    block_type: BlockType | None = None
    default_status: bool


class GlobalPolicyImport(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    plugins: dict[str, GlobalFeatureImport]


class GlobalTaskPolicyImport(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    tasks: dict[str, GlobalFeatureImport]


class PolicyDocument(BaseModel):
    version: int = Field(default=1, ge=1, le=1)
    scopes: list[ScopedPolicyImport] = Field(default_factory=list, max_length=256)
    accounts: list[AccountPolicyImport] = Field(default_factory=list, max_length=256)
    policies: list[TemplatePolicyImport] = Field(default_factory=list, max_length=256)
    global_policy: GlobalPolicyImport | None = None
    global_tasks: GlobalTaskPolicyImport | None = None

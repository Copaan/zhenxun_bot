from tortoise import fields

from zhenxun.services.db_context import Model


class MigrationImportRecord(Model):
    """迁移来源与目标记录的持久对应关系"""

    id = fields.CharField(64, pk=True)
    source_identity = fields.CharField(64)
    table_name = fields.CharField(255)
    source_pk = fields.TextField()
    target_pk = fields.TextField()

    class Meta:
        table = "migration_import_records"

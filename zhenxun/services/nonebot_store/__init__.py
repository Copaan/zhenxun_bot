"""Managed NoneBot Registry services.

Importing this package or its modules must not initialize NoneBot, open a database
connection, start a scheduler, or load a library plugin. The launcher relies on those
side-effect boundaries before ``nonebot.init()``.
"""

"""add uploader_id to file_metadata

Revision ID: b3e9f1a2c4d5
Revises: 966261d5a38c
Create Date: 2026-02-22 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b3e9f1a2c4d5"
down_revision: Union[str, Sequence[str], None] = "966261d5a38c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add uploader_id column to track who uploaded each file."""
    op.add_column(
        "file_metadata",
        sa.Column("uploader_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    """Remove uploader_id column."""
    op.drop_column("file_metadata", "uploader_id")

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""add index get_task_instances

Revision ID: d074d8839d6c
Revises: d2ae31099d61
Create Date: 2018-03-28 16:55:41.083602

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'd074d8839d6c'
down_revision = 'd2ae31099d61'
branch_labels = None
depends_on = None

INDEX_NAME = 'idx_ti_dag_execution_priority'
TABLE_NAME = 'task_instance'
INDEX_COLUMNS = ['dag_id', 'execution_date', 'priority_weight']


def upgrade():
    op.create_index(INDEX_NAME, TABLE_NAME, INDEX_COLUMNS)


def downgrade():
    op.drop_index(INDEX_NAME, TABLE_NAME)

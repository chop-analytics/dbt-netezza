import pytest
from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshotBase,
    BaseSimpleSnapshot,
    BaseSnapshotCheck,
)
from dbt.tests.util import relation_from_name, run_dbt
from dbt.tests.fixtures.project import TestProjInfo


# Override to fix 'drop table...if exists' SQL
def clone_table(
    project: TestProjInfo,
    to_table: str,
    from_table: str,
    select: str,
    where: str = None,
):
    """
    Creates a new table based on another table in a dbt project

    Args:
        project: the dbt project that contains the table
        to_table: the name of the table, without a schema, to be created
        from_table: the name of the table, without a schema, to be cloned
        select: the selection clause to apply on `from_table`; defaults to all columns (*)
        where: the where clause to apply on `from_table`, if any; defaults to all records
    """
    to_table_name = relation_from_name(project.adapter, to_table)
    from_table_name = relation_from_name(project.adapter, from_table)
    select_clause = select or "*"
    where_clause = where or "1 = 1"
    sql = f"drop table {to_table_name} if exists"
    project.run_sql(sql)
    sql = f"""
        create table {to_table_name} as
        select {select_clause}
        from {from_table_name}
        where {where_clause}
    """
    project.run_sql(sql)


class NetezzaSimpleSnapshotBase(BaseSimpleSnapshotBase):
    # Override to call updated version of 'clone_table'
    def create_fact_from_seed(self, where: str = None):
        clone_table(self.project, "fact", "seed", "*", where)


class TestSnapshotNetezza(NetezzaSimpleSnapshotBase, BaseSimpleSnapshot):
    # FIXME Refactor test to not add column or investigate transaction
    @pytest.mark.skip("Netezza does not support adding a column in a transaction block")
    def test_new_column_captured_by_snapshot(self, project):
        pass


class TestSnapshotCheckNetezza(NetezzaSimpleSnapshotBase, BaseSnapshotCheck):
    # Override to replace left() with substr()
    def test_column_selection_is_reflected_in_snapshot(self, project):
        """
        Update the first 10 records on a non-tracked column.
        Update the middle 10 records on a tracked column. (hence records 6-10 are updated on both)
        Show that all ids are current, and only the tracked column updates are reflected in `snapshot`.
        """
        self.update_fact_records(
            {"last_name": "substr(last_name, 0, 3)"}, "id between 1 and 10"
        )  # not tracked
        self.update_fact_records(
            {"email": "substr(email, 0, 3)"}, "id between 6 and 15"
        )  # tracked
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(6, 16),
        )

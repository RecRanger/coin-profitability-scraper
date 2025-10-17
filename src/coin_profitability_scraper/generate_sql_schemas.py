"""Generate SQL schemas from Dataframely schemas."""

from typing import Any

import sqlalchemy

from coin_profitability_scraper import PACKAGE_ROOT
from coin_profitability_scraper.minerstat.step_3b_ingest_each_coin_page import (
    DySchemaMinerstatCoins,
)

schema_output_folder = PACKAGE_ROOT.parent.parent / "dolt_schema"


def main() -> None:
    """Generate SQL schemas."""
    assert schema_output_folder.is_dir()

    for table_name, schema_class in (
        {
            "minerstat_coins": DySchemaMinerstatCoins,
        }
    ).items():
        sqlalchemy_columns: list[Any] = schema_class.sql_schema(  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
            dialect=sqlalchemy.Dialect(),
        )

        # Create a MetaData instance.
        metadata = sqlalchemy.MetaData()

        # Add automatic added and updated columns.
        sqlalchemy_columns.extend(
            [
                sqlalchemy.Column(
                    "created_at",
                    sqlalchemy.DateTime,
                    nullable=False,
                    # Set when the row is first inserted:
                    server_default=sqlalchemy.func.now(),
                ),
                sqlalchemy.Column(
                    "updated_at",
                    sqlalchemy.DateTime,
                    nullable=False,
                    server_default=sqlalchemy.text(
                        "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                    ),
                ),
            ]
        )

        # Dynamically create a Table from the list of columns.
        my_table = sqlalchemy.Table(table_name, metadata, *sqlalchemy_columns)

        # Generate the CREATE TABLE statement.
        create_stmt = str(
            sqlalchemy.schema.CreateTable(my_table).compile(
                compile_kwargs={"literal_binds": True}
            )
        ).replace("\t", " " * 4)
        (schema_output_folder / f"{table_name}.sql").write_text(create_stmt)


if __name__ == "__main__":
    main()

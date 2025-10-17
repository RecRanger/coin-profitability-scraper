"""Utilities for managing the Dolt database connection."""

import polars as pl
import sqlalchemy
import sqlalchemy.dialects.mysql
from tqdm import tqdm

DOLT_DATABASE_NAME = "cryptocurrency-coin-algo-data"
DOLT_REPO_URL = (
    "https://www.dolthub.com/repositories/recranger/cryptocurrency-coin-algo-data"
)


def upsert_polars_rows(
    engine: sqlalchemy.engine.Engine,
    table_name: str,
    df: pl.DataFrame,
    *,
    batch_size: int = 1000,
    exclude_float_columns_in_change_assessment: bool = True,
) -> None:
    """Upsert all rows from a Polars DataFrame into the given SQL table.

    Args:
        engine: SQLAlchemy Engine
        table_name (str): Name of the SQL table
        df (pl.DataFrame): Polars DataFrame
        batch_size (int): Size of each upsert operation.
        exclude_float_columns_in_change_assessment (bool): Whether to exclude
            float columns from the change assessment. Float cols ALWAYS show as
            changed.

    """
    # Filter the dataframe to updates only.
    # TODO: There may be a more efficent way relying on database features.
    df_current = pl.read_database(
        query=f"SELECT * FROM {table_name}",  # noqa: S608
        connection=engine,
    )
    # Cast so that join works, especially in case `df_current` is empty (brand new
    # table).
    df_current = df_current.cast(
        {col: dtype for col, dtype in df.schema.items() if col in df_current}
    )
    join_cols = sorted(set(df.columns) & set(df_current.columns))
    if exclude_float_columns_in_change_assessment:
        join_cols = [
            col
            for col in join_cols
            if df_current[col].dtype not in {pl.Float64, pl.Float32}
        ]
    df_update = df.join(
        df_current,
        on=join_cols,
        nulls_equal=True,  # Important.
        how="anti",
    )
    del df  # Ensure we always use `df_update` now.

    if df_update.height == 0:
        return

    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, meta, autoload_with=engine)

    # Create an INSERT ... ON DUPLICATE KEY UPDATE statement.
    stmt = sqlalchemy.dialects.mysql.insert(table)

    # Define what to do on duplicate key.
    update_dict = {
        c.name: stmt.inserted[c.name]
        for c in table.columns
        if (
            not c.primary_key
            # Explicitly exclude `created_at` to avoid updating it.
            and c.name not in {"created_at"}
            # Disable: `and c.name in df_update.columns` (`updated_at` doesn't update).
        )
    }
    stmt = stmt.on_duplicate_key_update(**update_dict)

    # Iterate over rows from Polars and update.
    with engine.begin() as conn:
        for df_chunk in tqdm(
            df_update.iter_slices(batch_size),
            desc=f'Upserting {df_update.height} rows to "{table_name}"',
            unit="batch",
            total=(df_update.height // batch_size + 1),
        ):
            conn.execute(stmt, df_chunk.to_dicts())

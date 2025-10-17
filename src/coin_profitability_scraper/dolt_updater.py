"""Tool for cloning, updating, and pushing a Dolt database."""

import random
import shutil
import subprocess
import tempfile
import time
from contextlib import AbstractContextManager
from pathlib import Path
from types import TracebackType

import sqlalchemy
from loguru import logger


class DoltDatabaseUpdater(AbstractContextManager["DoltDatabaseUpdater"]):
    """Context manager for temporarily cloning a Dolt database to update it.

    Handles cloning, running a Dolt SQL server, connecting to it via SQLAlchemy, and
    committing/pushing changes.
    """

    def __init__(self, repo_url: str) -> None:
        """Initialize the context manager."""
        self.repo_url: str = repo_url
        self._dolt_sql_username: str = "root"
        self._dolt_sql_host: str = "127.0.0.1"
        self.dolt_sql_port: int = random.randint(33000, 40000)  # noqa: S311
        self._dolt_sql_database_name: str = repo_url.split("/")[-1]

        self._proc = None

        self._dolt_command_path: str = "dolt"

    def __enter__(self) -> "DoltDatabaseUpdater":
        """Start the context manager."""
        # Step 1: Create a temp folder.
        self.dolt_clone_dir = (
            Path(tempfile.mkdtemp(prefix="dolt_repo_")) / self._dolt_sql_database_name
        )
        self.dolt_clone_dir.mkdir(parents=False, exist_ok=False)

        # Step 2: Clone the Dolt repo
        subprocess.run(  # noqa: S603
            [self._dolt_command_path, "clone", self.repo_url, self.dolt_clone_dir],
            check=True,
        )

        # Step 3: Start Dolt SQL server on a random port
        self._proc = subprocess.Popen(  # noqa: S603
            [
                self._dolt_command_path,
                "sql-server",
                "--host",
                self._dolt_sql_host,
                "--port",
                str(self.dolt_sql_port),
            ],
            cwd=self.dolt_clone_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Give Dolt a moment to start.
        time.sleep(2)

        # Step 4: Create SQLAlchemy engine.
        conn_str = f"mysql+pymysql://{self._dolt_sql_username}@{self._dolt_sql_host}:{self.dolt_sql_port}/{self._dolt_sql_database_name}"
        self.engine = sqlalchemy.create_engine(conn_str)

        # Test connection.
        number_of_tries = 10
        for try_num in range(number_of_tries):
            try:
                with self.engine.connect() as conn:
                    conn.execute(sqlalchemy.text("SELECT 1"))
                break
            except Exception:
                if try_num == number_of_tries - 1:
                    raise
                time.sleep(0.5)

        return self

    def dolt_commit_and_push(self, commit_message: str) -> None:
        """Stage, commit, and push changes to the Dolt remote.

        Equivalent to:
            dolt add .
            dolt commit -m "..."
            dolt push
        """
        if not self.dolt_clone_dir:
            msg = "Dolt repo not initialized"
            raise RuntimeError(msg)

        # Stage all changes
        subprocess.run(  # noqa: S603
            [self._dolt_command_path, "add", "."], cwd=self.dolt_clone_dir, check=True
        )

        # Commit
        subprocess.run(  # noqa: S603
            [self._dolt_command_path, "commit", "-m", commit_message],
            cwd=self.dolt_clone_dir,
            check=True,
        )

        # Push to remote.
        subprocess.run(  # noqa: S603
            [self._dolt_command_path, "push"], cwd=self.dolt_clone_dir, check=True
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """End the context manager."""
        # Step 6: Stop the Dolt SQL server
        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()

        # Step 7: Delete temp folder.
        if self.dolt_clone_dir and self.dolt_clone_dir.exists():
            shutil.rmtree(self.dolt_clone_dir)

        return False  # Do not suppress exceptions.


def _demonstrate_usage() -> None:
    """Demonstrate usage of the DoltDatabaseUpdater context manager."""
    with DoltDatabaseUpdater(
        "https://www.dolthub.com/repositories/post-no-preference/rates"
    ) as db:
        logger.info(f"Connected to Dolt on port {db.dolt_sql_port}")

        with db.engine.begin() as conn:
            logger.info("Showing tables:")
            logger.info(conn.exec_driver_sql("SHOW TABLES").fetchall())

            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE cars "
                    "(id INT, make VARCHAR(50), model VARCHAR(50), year INT)"
                )
            )

            # Insert or update data
            conn.exec_driver_sql(
                "INSERT INTO cars (id, make, model, year) "
                "VALUES (1000, 'Tesla', 'Model Y', 2025)"
            )

            # Query again
            result = conn.exec_driver_sql("SELECT * FROM cars WHERE id = 1000")
            logger.info(result.fetchall())

            # Commit and push changes
            db.dolt_commit_and_push("Update cars table.")


if __name__ == "__main__":
    _demonstrate_usage()

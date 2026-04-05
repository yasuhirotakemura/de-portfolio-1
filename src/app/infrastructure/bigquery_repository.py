import logging
import pandas as pd

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryRepository:
    def __init__(
        self,
        project_id: str,
    ) -> None:
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def load_dataframe(
        self, df: pd.DataFrame, table_id: str, schema: list[bigquery.SchemaField]
    ) -> None:
        logger.info(
            "データフレームをBigQueryにロードします。table_id=%s, rows=%d",
            table_id,
            len(df),
        )
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
        )

        job = self.client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config,
        )
        job.result()

        logger.info(
            "データフレームをBigQueryにロードしました。table_id=%s, rows=%d",
            table_id,
            len(df),
        )

import pandas as pd

from app.conifg.config import get_settings
from app.conifg.logging_config import setup_logging
from app.domain.entities.location import Location
from app.schemas.raw import open_meteo_hourly_schema
from app.infrastructure.bigquery_repository import BigQueryRepository
from app.infrastructure.open_meteo_repository import OpenMeteoRepository

setup_logging()


LOCATIONS: list[Location] = [
    Location(
        location_id="tokyo",
        location_name="東京",
        latitude=35.694004,
        longitude=139.753632,
    )
]


def main() -> None:
    [app_env, project_id] = get_settings()

    all_dfs: list[pd.DataFrame] = []

    # 各種リポジトリの初期化
    bigquery_repository = BigQueryRepository(
        project_id=project_id,
    )
    open_meteo_repository = OpenMeteoRepository()

    # 各ロケーションのデータを取得してDataFrameに格納
    for location in LOCATIONS:
        df = open_meteo_repository.fetch_open_meteo_hourly(location)
        all_dfs.append(df)

    final_df = pd.concat(all_dfs, ignore_index=True)
    bigquery_repository.load_dataframe(
        final_df,
        table_id="raw.open_meteo_hourly",
        schema=open_meteo_hourly_schema.open_meteo_hourly,
    )


if __name__ == "__main__":
    main()

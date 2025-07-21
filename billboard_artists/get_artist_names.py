import billboard
from error_logging.base_logger import logger
from .database_structure import BillboardArist


def get_artist_names():
    """
    Fetches artists names from the Billboard artist-100 chart and stores them in a table on iceberg
    """

    chart = billboard.ChartData('artist-100', date='2022-12-31')

    return chart

def record_artist_names():
    """
    Records artist names from the Billboard artist-100 chart into the database.
    """
    chart = get_artist_names()
    if not chart:
        logger.error("No artists found in the chart.")
        return

    artists = [{'artist_name': entry.artist} for entry in chart]
    
    BillboardArist.create_table('artist_names', artists)
    logger.info("Artist names successfully recorded in the database.")



if __name__ == "__main__":
    try:
        record_artist_names()
    except Exception as e:
        logger.error(f"An error occurred while recording artist names: {e}")
        raise
    else:
        logger.info("Artist names recorded without errors.")
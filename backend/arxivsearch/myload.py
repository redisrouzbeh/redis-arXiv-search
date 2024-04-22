#!/usr/bin/env python3
import numpy as np
import json
import os
import logging

from typing import Any, Dict, List

from redisvl.index import SearchIndex

from arxivsearch import config
from arxivsearch.schema import Provider


logger = logging.getLogger(__name__)


def read_paper_json() -> List[Dict[str, Any]]:
    """
    Load JSON array of arXiv papers and embeddings.
    """
    logger.info("Loading papers dataset from disk")
    path = os.path.join(
        config.DATA_LOCATION, config.DEFAULT_DATASET
    )
    with open(path, "r") as f:
        df = json.load(f)
    return df


def write_sync(index: SearchIndex, papers: list):
    """
    Write arXiv paper records to Redis asynchronously.
    """
    def preprocess_paper(paper: dict) -> dict:
        #for provider_vector in Provider:
        #    paper[provider_vector] = np.array(
        #        paper[provider_vector], dtype=np.float32).tolist()#.tobytes()
        #paper['paper_id'] = paper.pop('id')
        #paper['id'] = paper['paper_id']
        paper['categories'] = paper['categories'].replace(",", "|")
        return paper

    logger.info("Loading papers dataset to Redis")

    keys = index.load(
        data=papers,
        preprocess=preprocess_paper,
        id_field="id"
    )

    logger.info("All papers loaded")
    return len(keys)


def load_data():
    # Load schema specs and create index in Redis
    try:
        index = SearchIndex.from_yaml(os.path.join("./schema", "index.yaml"))
        index.connect(redis_url=config.REDIS_URL)
        # create the index
        index.create(overwrite=True)
        if index.exists():
            print(f"Successfully created index={index.info}")
        papers = read_paper_json()
        print(f"About to upload {len(papers)} papers")
        num = write_sync(index=index, papers=papers)
        print(f"uploaded {num} papers")
    except Exception as e:
        print("An exception occurred while trying to load the index and dataset")

    # Wait for any indexing to finish
    #while True:
    #    info = index.info()
    #    if info["percent_indexed"] == "1":
    #        logger.info("Indexing is complete!")
    #        break
    #    logger.info(f"{info['percent_indexed']} indexed...")
    #    asyncio.sleep(5)


if __name__ == "__main__":
    #asyncio.run()
    load_data()